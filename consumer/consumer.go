package consumer

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bitleak/kaproxy/config"
	"github.com/bitleak/kaproxy/log"
	"github.com/bitleak/kaproxy/util"
	consumergroup "github.com/meitu/go-consumergroup"
	"github.com/meitu/go-zookeeper/zk"
	"github.com/meitu/zk_wrapper"
	"github.com/sirupsen/logrus"
)

type Consumer struct {
	proxyID      string
	saramaClient sarama.Client
	zkCli        *zk_wrapper.Conn
	conf         *config.Config

	groups     sync.Map
	Puller     *Puller
	controller *controller
	metadataCh chan *zk.Event
	stopper    chan struct{}
}

func NewConsumer(saramaClient sarama.Client, conf *config.Config) (*Consumer, error) {
	proxyID, err := genProxyID(conf.Server.Port)
	if err != nil {
		return nil, fmt.Errorf("generate proxy id encounter error: %s", err)
	}
	c := &Consumer{
		proxyID:      proxyID,
		saramaClient: saramaClient,
		conf:         conf,
		metadataCh:   make(chan *zk.Event, 1024),
		stopper:      make(chan struct{}),
	}

	callback := getMetadataCallback(c)
	c.zkCli, err = newZKCli(conf, callback)
	if err != nil {
		return nil, fmt.Errorf("failed to new zk client: %s", err)
	}

	c.Puller, err = newPuller(c)
	if err != nil {
		return nil, fmt.Errorf("failed to init puller: %s", err)
	}
	c.controller = newController(c)
	return c, nil
}

func (c *Consumer) Start() error {
	go c.watchConsumerGroup()
	go c.handleMetadataChange()

	err := c.Puller.Start()
	if err != nil {
		return fmt.Errorf("failed to start puller: %s", err)
	}

	go c.controller.start()
	return nil
}

func (c *Consumer) Stop() {
	c.ReleaseConsumerGroups()
	err := c.Puller.Close()
	if err != nil {
		log.Logger.Errorf("puller stop failed, %s", err.Error())
	} else {
		log.Logger.Info("puller was stopped")
	}

	err = c.controller.stop()
	if err != nil {
		log.Logger.Errorf("Failed to stop monitor: %s", err)
	} else {
		log.Logger.Info("monitor is stopped")
	}
	close(c.stopper)
	c.zkCli.Close()
}

func (c *Consumer) watchConsumerGroup() {
	defer util.WithRecover(log.Logger)

	for {
		newGroupList, _, groupListChange, err := c.zkCli.ChildrenW(consumerGroupsDir)
		if err != nil {
			log.Logger.WithField("err", err).Warn("Failed to get and watch consumer group list")
			time.Sleep(3 * time.Second)
			continue
		}

		for _, name := range newGroupList {
			if _, ok := c.groups.Load(name); ok {
				continue
			}
			c.startGroup(name)
		}

		//fixme: double cycle is too inefficient
		c.groups.Range(func(key, value interface{}) bool {
			exist := false
			for _, group := range newGroupList {
				if group == key.(string) {
					exist = true
					break
				}
			}
			if !exist {
				c.delGroup(key.(string))
			}
			return true
		})

		<-groupListChange.EvCh
	}
}

func (c *Consumer) GetConsumerGroup(group string) *consumerGroup {
	if cg, ok := c.groups.Load(group); ok {
		return cg.(*consumerGroup)
	}
	return nil
}

func (c *Consumer) GetMetadataByName(group string) *GroupMetadata {
	if cg, ok := c.groups.Load(group); ok {
		return cg.(*consumerGroup).metadata
	}
	return nil
}

func (c *Consumer) ListConsumerGroup() []string {
	groups := make([]string, 0)
	c.groups.Range(func(key, value interface{}) bool {
		groups = append(groups, key.(string))
		return true
	})
	return groups
}

func (c *Consumer) ReleaseConsumerGroups() {
	c.groups.Range(func(key, value interface{}) bool {
		cg := value.(*consumerGroup)
		if !cg.IsStopped() {
			cg.Stop()
		}
		return true
	})
}

func (c *Consumer) GetConsumerGroupState(group string) (string, error) {
	cg := c.GetConsumerGroup(group)
	if cg == nil {
		return "", fmt.Errorf("group [%s] is not found", group)
	}
	if cg.IsStopped() {
		return "stopped", nil
	}
	return "started", nil
}

func (c *Consumer) updateGroupMetadataToZK(group string, metadata *GroupMetadata) error {
	bytes, _ := json.Marshal(metadata)
	_, err := c.zkCli.Set("/consumers/"+group, bytes, -1)
	return err
}

func (c *Consumer) StartConsumerGroup(group string) error {
	cg := c.GetConsumerGroup(group)
	if cg == nil {
		return fmt.Errorf("group [%s] is not found", group)
	}
	if !cg.IsStopped() {
		return fmt.Errorf("consumer group is running")
	}

	cg.metadata.Stopped = false
	err := c.updateGroupMetadataToZK(group, cg.metadata)
	if err != nil {
		log.Logger.WithFields(logrus.Fields{
			"group": group,
			"err":   err,
		}).Error("Failed to update the metadata to zk")
		return err
	}
	c.startGroup(group)
	return nil
}

func (c *Consumer) StopConsumerGroup(group string) error {
	cg := c.GetConsumerGroup(group)
	if cg == nil {
		return fmt.Errorf("group [%s] is not found", group)
	}
	if cg.IsStopped() {
		return fmt.Errorf("consumer group is stopped or rebalancing")
	}
	cg.metadata.Stopped = true
	err := c.updateGroupMetadataToZK(group, cg.metadata)
	if err != nil {
		log.Logger.WithFields(logrus.Fields{
			"group": group,
			"err":   err,
		}).Error("Failed to update the metadata to zk")
		return err
	}
	cg.Stop()
	return nil
}

func (c *Consumer) blockingConsume(messages <-chan *sarama.ConsumerMessage, timeout time.Duration) (*sarama.ConsumerMessage, error) {
	timer := time.NewTimer(timeout)
	select {
	case message := <-messages:
		return message, nil
	case <-timer.C:
		return nil, ErrNoMessage
	}

}

func (c *Consumer) noBlockingConsume(messages <-chan *sarama.ConsumerMessage) (*sarama.ConsumerMessage, error) {
	select {
	case message := <-messages:
		return message, nil
	default:
		return nil, ErrNoMessage
	}
}

func (c *Consumer) Consume(group, topic string, timeout, ttr time.Duration) (message *sarama.ConsumerMessage, err error) {
	cg := c.GetConsumerGroup(group)
	if cg == nil {
		return nil, ErrGroupNotFound
	}
	if cg.IsStopped() {
		return nil, ErrGroupStopped
	}
	var (
		messages <-chan *sarama.ConsumerMessage
		ok       bool
	)
	if cg.metadata.Semantics == SemanticAtLeastOnce {
		um := cg.metadata.unackManager
		if um.isStop {
			return nil, ErrNoMessage
		}
		defer func() {
			if message != nil {
				um.push(message, time.Now().Add(ttr))
			}
		}()
		message = um.pop(topic)
		if message != nil {
			return message, nil
		}
		messages, ok = um.getMessages(topic)
		if !ok {
			return nil, ErrTopicNotFound
		}
	} else {
		if messages, ok = cg.GetMessages(topic); !ok {
			return nil, ErrTopicNotFound
		}
	}

	if cg.getPartitionNum(topic) <= 0 {
		return nil, ErrNoPartition
	}

	if timeout == 0 {
		return c.noBlockingConsume(messages)
	}
	return c.blockingConsume(messages, timeout)
}

func (c *Consumer) ACK(token, group, topic string, partition int32, offset int64) error {
	cg := c.GetConsumerGroup(group)
	if cg == nil {
		return ErrGroupNotFound
	}
	if cg.IsStopped() {
		return ErrGroupStopped
	}
	if cg.metadata.Semantics == SemanticAtMostOnce {
		return ErrGroupNotAllowACK
	}
	um := cg.metadata.unackManager
	if um.isStop {
		return ErrUnackMessageNotFound
	}

	newOffset, err := um.ack(token, topic, partition, offset)
	if err != nil {
		return err
	}
	if newOffset > offset {
		cg.CommitOffset(topic, partition, newOffset)
	}
	return nil
}

func (c *Consumer) Export(group string) (map[string]interface{}, error) {
	cg := c.GetConsumerGroup(group)
	if cg == nil {
		return nil, ErrGroupNotFound
	}
	info := make(map[string]interface{})
	info["metadata"] = cg.metadata
	info["offsets"] = cg.GetOffsets()
	return info, nil
}

func (c *Consumer) handleMetadataChange() {
	for event := range c.metadataCh {
		select {
		case <-c.stopper:
			return
		default:
		}

		path := c.zkCli.TrimChroot(event.Path)
		arr := strings.Split(path, "consumers/")
		if len(arr) < 2 {
			continue
		}
		group := arr[1]
		c.delGroup(group)
		c.startGroup(group)
	}
}

// new or update a consumer group and than start it.
func (c *Consumer) startGroup(group string) {
	path := fmt.Sprintf(consumerGroupsPath, group)
	data, _, _, err := c.zkCli.GetW(path)
	if len(data) == 0 {
		return
	}
	if err != nil {
		log.Logger.WithFields(logrus.Fields{
			"err":   err,
			"group": group,
		}).Warn("Failed to get and watch consumer group")
		return
	}

	metadata := &GroupMetadata{
		Consumer: c.conf.Consumer,
	}
	if err := json.Unmarshal(data, metadata); err != nil {
		log.Logger.WithFields(logrus.Fields{
			"err":   err,
			"group": group,
		}).Warn("Failed to unmarshal group metadata")
		return
	}
	if metadata.Owner != OwnerName {
		log.Logger.WithField("group", group).Warn(
			"Failed to start consumer group, because the owner of consumer group isn't kaproxy")
		return
	}

	consumerID := genConsumerID(c.proxyID, group)
	cg, err := newConsumerGroup(c.conf, group, consumerID, metadata, c.zkCli)
	if err != nil {
		log.Logger.WithField("err", err).Warn("Failed to new consumer group")
		return
	}
	c.groups.Store(group, cg)
	if metadata.Stopped {
		log.Logger.WithField("group", group).Info("The group was stopped")
		return
	}
	go cg.Start()
	for _, topic := range metadata.Topics {
		go func(topic string) {
			errChan, _ := cg.GetErrors(topic)
			for err := range errChan {
				log.Logger.WithFields(logrus.Fields{
					"err":   err,
					"group": group,
				}).Error("received sarama error")
			}
		}(topic)
	}
}

func (c *Consumer) delGroup(group string) {
	cg := c.GetConsumerGroup(group)
	if cg == nil {
		return
	}
	if !cg.IsStopped() {
		cg.Stop()
	}
	c.groups.Delete(group)
	log.Logger.WithField("group", group).Error("Group is stop, because zk dir is deleted")
}

//note: EventCallback will block the subsequent operations,
//so the callback function uses an asynchronous call
func getMetadataCallback(c *Consumer) zk.EventCallback {
	return func(event zk.Event) {
		if event.Type == zk.EventNodeDataChanged {
			c.metadataCh <- &event
		}
	}
}

func newZKCli(conf *config.Config, cb zk.EventCallback) (*zk_wrapper.Conn, error) {
	conn, _, err := zk.Connect(conf.Kafka.Zookeepers, conf.Kafka.ZKSessionTimeout.Duration, zk.WithEventCallback(cb))
	if err != nil {
		return nil, err
	}
	zkCli := &zk_wrapper.Conn{Conn: conn}
	if err = zkCli.Chroot(conf.Kafka.ZKChroot); err != nil {
		return nil, err
	}
	return zkCli, nil
}

//ConsumerID is set so that the instances are not sorted in a fixed order,
//and the partitions consumption is more balanced.
//The ConsumerID is a combination of a prefix and a proxyID,
//where the prefix is hash of the proxyID and group.
//Format: {prefix}-{host:port}-{timestamp}

func genConsumerID(proxyID, group string) string {
	hash := md5.Sum(append([]byte(proxyID), []byte(group)...))
	prefix := fmt.Sprintf("{%x}-", hash[:4])
	return prefix + proxyID
}

type consumerGroup struct {
	*consumergroup.ConsumerGroup
	metadata *GroupMetadata

	partitionNums map[string]int
	lastUpdate    time.Time
}

func (cg *consumerGroup) getPartitionNum(topic string) int {
	if cg.lastUpdate.Before(time.Now().Add(-1 * time.Second)) {
		offsets := cg.GetOffsets()
		partitionNums := make(map[string]int)
		for topic, partitions := range offsets {
			partitionNums[topic] = len(partitions.(map[int32]interface{}))
		}
		cg.partitionNums = partitionNums
		cg.lastUpdate = time.Now()
	}
	num, ok := cg.partitionNums[topic]
	if !ok {
		return 0
	}
	return num
}

func newConsumerGroup(proxyConf *config.Config, groupID, consumerID string, metadata *GroupMetadata, zkCli *zk_wrapper.Conn) (*consumerGroup, error) {
	conf := consumergroup.NewConfig()
	conf.SaramaConfig.Version = proxyConf.Kafka.Version.Version
	conf.ZkList = proxyConf.Kafka.Zookeepers
	conf.ZkSessionTimeout = proxyConf.Kafka.ZKSessionTimeout.Duration
	conf.Chroot = proxyConf.Kafka.ZKChroot
	conf.GroupID = groupID
	conf.ConsumerID = consumerID
	conf.ClaimPartitionRetryTimes = 0

	//load consumer metadata
	conf.TopicList = metadata.Topics
	conf.OffsetAutoCommitInterval = metadata.Consumer.OffsetAutoCommitInterval.Duration
	conf.OffsetAutoReset = metadata.Consumer.OffsetAutoReset.Reset
	conf.ClaimPartitionRetryInterval = metadata.Consumer.ClaimPartitionRetryInterval.Duration
	conf.OffsetAutoCommitEnable = metadata.Semantics == SemanticAtMostOnce
	conf.SaramaConfig.ChannelBufferSize = metadata.Consumer.ChannelBufferSize
	conf.SaramaConfig.Consumer.Fetch.Default = metadata.Consumer.FetchDefault
	conf.SaramaConfig.Consumer.MaxWaitTime = metadata.Consumer.MaxWaitTime.Duration
	conf.SaramaConfig.Consumer.MaxProcessingTime = metadata.Consumer.MaxProcessingTime.Duration

	cg, err := consumergroup.NewConsumerGroup(conf)
	if err != nil {
		return nil, err
	}
	cg.SetLogger(log.Logger)
	if metadata.Semantics == SemanticAtLeastOnce {
		metadata.unackManager = newUnackManager(groupID, consumerID, proxyConf.Consumer.UnackManagerSize, zkCli, cg)
		cg.OnLoad(func() {
			owners := cg.Owners()
			chMap := make(map[string]<-chan *sarama.ConsumerMessage)
			for _, topic := range conf.TopicList {
				chMap[topic], _ = cg.GetMessages(topic)
			}
			metadata.unackManager.start(owners, chMap)
		})
		cg.OnClose(metadata.unackManager.stop)
	}
	return &consumerGroup{
		cg,
		metadata,
		make(map[string]int),
		time.Now().Add(-1 * time.Second),
	}, nil
}
