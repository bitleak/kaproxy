package consumer

import (
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/bitleak/kaproxy/log"
	"github.com/bitleak/kaproxy/util"

	"github.com/Shopify/sarama"
	"github.com/meitu/go-consumergroup"
	"github.com/meitu/zk_wrapper"
)

type unackMessage struct {
	Message *sarama.ConsumerMessage
	Expire  time.Time
}

type messageHeap struct {
	messages  []*unackMessage
	offsetMap *sync.Map // key is "partition-offset", value is index of messages
}

func (h messageHeap) Len() int           { return len(h.messages) }
func (h messageHeap) Less(i, j int) bool { return h.messages[i].Expire.Before(h.messages[j].Expire) }
func (h messageHeap) Swap(i, j int) {
	a, b := h.messages[i], h.messages[j]
	h.offsetMap.Store(h.genKey(a.Message.Partition, a.Message.Offset), j)
	h.offsetMap.Store(h.genKey(b.Message.Partition, b.Message.Offset), i)
	h.messages[i], h.messages[j] = b, a
}

func (h *messageHeap) Push(x interface{}) {
	message := x.(*unackMessage)
	h.offsetMap.Store(h.genKey(message.Message.Partition, message.Message.Offset), h.Len())
	h.messages = append(h.messages, message)
}

func (h *messageHeap) Pop() interface{} {
	x := (h.messages)[len(h.messages)-1]
	h.messages = (h.messages)[:len(h.messages)-1]
	h.offsetMap.Delete(h.genKey(x.Message.Partition, x.Message.Offset))
	return x
}

func (h *messageHeap) push(message *sarama.ConsumerMessage, expire time.Time) {
	heap.Push(h, &unackMessage{message, expire})
}

func (h *messageHeap) pop() *sarama.ConsumerMessage {
	if h.Len() == 0 {
		return nil
	}
	um := h.messages[0]
	if um.Expire.Before(time.Now()) {
		return heap.Pop(h).(*unackMessage).Message
	}
	return nil
}

func (h *messageHeap) remove(partition int32, offset int64) error {
	key := h.genKey(partition, offset)
	index, ok := h.offsetMap.Load(key)
	if !ok {
		return ErrUnackMessageNotFound
	}
	heap.Remove(h, index.(int))
	return nil
}

func (h messageHeap) genKey(partition int32, offset int64) string {
	return fmt.Sprintf("%d-%d", partition, offset)
}

type queue struct {
	elements []*struct{}
	len, end int
}

type offsets struct {
	queues    map[int32]*queue
	size, len int
	nonFull   chan struct{}
}

func newOffsets(partitions []int32, size int) *offsets {
	queues := make(map[int32]*queue)
	for _, p := range partitions {
		queues[p] = &queue{
			elements: make([]*struct{}, size),
		}
	}
	offsets := &offsets{
		queues:  queues,
		size:    size,
		nonFull: make(chan struct{}),
	}
	close(offsets.nonFull)
	return offsets
}

func (o *offsets) enqueue(partition int32, offset int64) {
	queue := o.queues[partition]
	i := int(offset % int64(o.size))
	queue.elements[i] = &struct{}{}
	queue.end = i
	queue.len++
	o.len++
	if o.len == o.size {
		o.nonFull = make(chan struct{})
	}
}

func (o *offsets) dequeue(partition int32, offset int64) int64 {
	queue := o.queues[partition]
	i := int(offset % int64(o.size))
	queue.elements[i] = nil
	start := (queue.end - queue.len + 1 + o.size) % o.size
	if i == start {
		queue.len--
		o.len--
		offset++
		for {
			next := (i + 1) % o.size
			if i == queue.end || queue.elements[next] != nil {
				break
			}
			i = next
			queue.len--
			o.len--
			offset++
		}
		select {
		case <-o.nonFull:
		default:
			close(o.nonFull)
		}
	}
	return offset
}

type TopicUnackManager struct {
	heap      *messageHeap
	offsets   *offsets
	quit      chan struct{}
	messageCh chan *sarama.ConsumerMessage
	lock      sync.Mutex
}

func newTopicUnackManager(partitions []int32, size int) *TopicUnackManager {
	return &TopicUnackManager{
		heap: &messageHeap{
			messages:  make([]*unackMessage, 0, size),
			offsetMap: new(sync.Map),
		},
		offsets:   newOffsets(partitions, size),
		messageCh: make(chan *sarama.ConsumerMessage),
		quit:      make(chan struct{}),
	}
}

func (t *TopicUnackManager) stop() {
	close(t.quit)
}

func (t *TopicUnackManager) transfer(messageCh <-chan *sarama.ConsumerMessage) {
	for message := range messageCh {
		select {
		case <-t.quit:
			return
		case <-t.offsets.nonFull:
			select {
			case t.messageCh <- message:
				t.lock.Lock()
				t.offsets.enqueue(message.Partition, message.Offset)
				t.lock.Unlock()
			case <-t.quit:
			}
		}
	}
}

func (t *TopicUnackManager) push(message *sarama.ConsumerMessage, expire time.Time) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.heap.push(message, expire)
}

func (t *TopicUnackManager) ack(partition int32, offset int64) (int64, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	err := t.heap.remove(partition, offset)
	if err != nil {
		return 0, err
	}
	return t.offsets.dequeue(partition, offset), nil
}

func (t *TopicUnackManager) pop() *sarama.ConsumerMessage {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.heap.pop()
}

type unackManager struct {
	group, consumerID string
	topicManagers     map[string]*TopicUnackManager
	owners            map[string]map[int32]string
	size              int
	isStop            bool
	wg                sync.WaitGroup
}

func newUnackManager(group, consumerID string, size int, zkCli *zk_wrapper.Conn, cg *consumergroup.ConsumerGroup) *unackManager {
	return &unackManager{
		topicManagers: make(map[string]*TopicUnackManager),
		consumerID:    consumerID,
		group:         group,
		size:          size,
		isStop:        true,
	}
}

//run after consumer group start
func (u *unackManager) start(owners map[string]map[int32]string, chs map[string]<-chan *sarama.ConsumerMessage) {
	for topic, partitionOwners := range owners {
		partitions := make([]int32, 0)
		for partition, owner := range partitionOwners {
			if owner == u.consumerID {
				partitions = append(partitions, partition)
			}
		}

		tm := newTopicUnackManager(partitions, u.size)
		u.topicManagers[topic] = tm
		u.wg.Add(1)
		go func(messageCh <-chan *sarama.ConsumerMessage) {
			defer util.WithRecover(log.Logger)
			defer u.wg.Done()
			tm.transfer(messageCh)
		}(chs[topic])
	}
	u.owners = owners
	u.isStop = false
}

//run before consumer group stop
func (u *unackManager) stop() {
	u.isStop = true
	for _, tm := range u.topicManagers {
		tm.stop()
	}
	u.wg.Wait()
}

func (u *unackManager) getMessages(topic string) (<-chan *sarama.ConsumerMessage, bool) {
	tm, ok := u.topicManagers[topic]
	if !ok {
		return nil, ok
	}
	return tm.messageCh, ok
}

func (u *unackManager) retransmit(addr, token, group, topic string, partition int32, offset int64) error {
	resp, err := http.PostForm(fmt.Sprintf("http://%s/group/%s/topic/%s/ack?token=%s", addr, group, topic, token),
		url.Values{"partition": {strconv.Itoa(int(partition))}, "offset": {strconv.FormatInt(offset, 10)}})
	if err != nil {
		return ErrUnackMessageNotFound
	}
	if resp.StatusCode == http.StatusOK {
		return nil
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	var msg struct{ Error string }
	json.Unmarshal(body, &msg)
	return errors.New(msg.Error)
}

func (u *unackManager) ack(token, topic string, partition int32, offset int64) (int64, error) {
	if _, ok := u.owners[topic]; !ok {
		return 0, ErrTopicNotFound
	}
	owner, ok := u.owners[topic][partition]
	if !ok {
		return 0, ErrUnackMessageNotFound
	}

	if owner != u.consumerID {
		addr := getAddrFromOwner(owner)
		return 0, u.retransmit(addr, token, u.group, topic, partition, offset)
	}

	return u.topicManagers[topic].ack(partition, offset)
}

func (u *unackManager) pop(topic string) *sarama.ConsumerMessage {
	tm := u.topicManagers[topic]
	if tm != nil {
		return tm.pop()
	}
	return nil
}

func (u *unackManager) push(message *sarama.ConsumerMessage, expire time.Time) {
	tm := u.topicManagers[message.Topic]
	if tm != nil {
		tm.push(message, expire)
	}
}
