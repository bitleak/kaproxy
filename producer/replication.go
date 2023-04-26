package producer

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
	"time"

	"github.com/bitleak/kaproxy/config"
	"github.com/bitleak/kaproxy/log"
	"github.com/bitleak/kaproxy/metrics"
	"github.com/bitleak/kaproxy/util"
	"github.com/bitleak/kaproxy/xor"
	"github.com/meitu/go-zookeeper/zk"
	"github.com/meitu/zk_wrapper"
	"github.com/sirupsen/logrus"
)

const (
	ReplicationConfigDir = "/proxy/replication"
)

type Replicator interface {
	Setup(producer *Producer)
	Close()
	Replicate(msg *ReplicatedMessage) error
	Allow(topic string) bool
}

type ReplicatedMessage struct {
	BatchId       string
	MetaMessage   *ReplicationMetaMessage
	SrcDatacenter string
	DstDatacenter string // Used by airbus
	MsgID         string

	Topic           string
	Key             []byte // Q: Why using []byte instead of string ?
	Headers         []sarama.RecordHeader
	Value           []byte // A: string when marshaled is required to be UTF-8, but []byte supports binary
	Partition       int32
	PartitionMethod string
}

type MetaMessageType byte

const (
	MetaStart MetaMessageType = iota
	MetaStop
)

type ReplicationMetaMessage struct {
	CheckSum string // md5, checksum of all messages between meta START message and STOP message
	Total    int    // batch size
	Type     MetaMessageType
}

type replicator struct {
	MyDatacenter     string
	ReplicationTopic string
	SyncInterval     time.Duration

	batchSize int
	zkCli     *zk_wrapper.Conn
	topicAcl  map[string]bool
	batch     *batchReplicator
	producer  *Producer
	genMsgID  func() string
}

type batchReplicator struct {
	replicator *replicator
	size       int
	counter    map[int32]int
	current    map[int32]string
	digestCh   map[int32]chan []byte
	mu         sync.Mutex
	shutdown   chan struct{}
	wg         sync.WaitGroup
}

func NewBatchReplicator(r *replicator, batchSize int) *batchReplicator {
	if batchSize <= 0 {
		log.ErrorLogger.Fatal("Invalid batch size")
		return nil
	}
	return &batchReplicator{
		replicator: r,
		size:       batchSize,
		counter:    make(map[int32]int),
		current:    make(map[int32]string),
		digestCh:   make(map[int32]chan []byte),
		shutdown:   make(chan struct{}),
	}
}

func (b *batchReplicator) batchWorker(partitionID int32, batchID, key string, digestCh chan []byte) func() {
	log.ErrorLogger.Debugf("Batch [%s] worker start", batchID)
	b.wg.Add(1)
	metrics.Batch.Start.Inc()
	b.sendMetaMessage(partitionID, batchID, key, MetaStart, "")
	checksum := xor.New()
	return func() {
		defer util.WithRecover(log.ErrorLogger, b.wg.Done)
	UserMsg:
		for i := 0; i < b.size; i++ {
			select {
			case d := <-digestCh:
				checksum.Write(d)
			case <-b.shutdown:
				// when shutdown, the MetaStop must be sent to avoid incomplete batch
				break UserMsg
			}
		}
		close(digestCh)
		b.sendMetaMessage(partitionID, batchID, key, MetaStop, base64.StdEncoding.EncodeToString(checksum.Sum(nil)))
		log.ErrorLogger.Debugf("Batch [%s] worker stop", batchID)
		metrics.Batch.Stop.Inc()
	}
}

func (b *batchReplicator) sendMetaMessage(partitionID int32, batchID, key string, typ MetaMessageType, checksum string) error {
	r := b.replicator
	p := r.producer
	msg := ReplicatedMessage{
		BatchId:       batchID,
		SrcDatacenter: r.MyDatacenter,
		MetaMessage: &ReplicationMetaMessage{
			Type:     typ,
			Total:    b.size,
			CheckSum: checksum,
		},
	}
	value, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %s", err)
	}
	_, err = p.ProduceMessageWithoutReplication(
		r.ReplicationTopic,
		key,
		string(value),
		nil,
		partitionID,
	)
	if err != nil {
		return fmt.Errorf("failed to send message: %s", err)
	}
	return nil
}

// Get Batch returns the batch ID for sending message. this is the main bottleneck
// of concurrency, after this call different batches can send message concurrently.
// BatchID is unique across replication topic's partitions.
func (b *batchReplicator) GetBatch(partitionID int32, key string) (batchID string, digestCh chan []byte, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for {
		if b.counter[partitionID] == 0 {
			log.ErrorLogger.Debug("New batch")
			b.current[partitionID] = genBatchId()
			ch := make(chan []byte, 5)
			b.digestCh[partitionID] = ch
			go b.batchWorker(partitionID, b.current[partitionID], key, ch)()
		}
		b.counter[partitionID]++
		if b.counter[partitionID] > b.size {
			b.counter[partitionID] = 0
			b.current[partitionID] = ""
			b.digestCh[partitionID] = nil
			continue // to new batch
		}
		return b.current[partitionID], b.digestCh[partitionID], nil
	}
}

// Send message and tell batchWorker to update the checksum
// messages of the same batch will be delivered out of order, so using checksum like md5 is
// impossible, so we use XOR method which is commutative and associative
func (b *batchReplicator) Send(partitionID int32, batchID, key string, digestCh chan []byte, msg *ReplicatedMessage) error {
	r := b.replicator
	p := r.producer

	msg.BatchId = batchID
	value, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %s", err)
	}
	_, err = p.ProduceMessageWithoutReplication(r.ReplicationTopic, key, string(value), msg.Headers, partitionID)
	if err != nil {
		return fmt.Errorf("failed to send message: %s", err)
	}
	// Send msg value to batchworker for checksum
	digestCh <- msg.Value
	return nil
}

// Stop all batchWorker gracefully
func (b *batchReplicator) Close() {
	close(b.shutdown)
	b.wg.Wait()
}

func NewReplicator(conf *config.ReplicationConfig, kafka *config.KafkaConfig) (*replicator, error) {
	zkCli, _, err := zk_wrapper.Connect(kafka.Zookeepers, kafka.ZKSessionTimeout.Duration)
	if err != nil {
		return nil, err
	}
	if kafka.ZKChroot != "" {
		zkCli.Chroot(kafka.ZKChroot)
	}
	genMsgIdFunc := util.GenUniqueID
	r := &replicator{
		MyDatacenter:     conf.MyDatacenterName,
		ReplicationTopic: conf.CrossDatacenterReplicationTopic,
		SyncInterval:     conf.ReplicationAclUpdateInterval.Duration,

		batchSize: conf.BatchChecksumSize,
		zkCli:     zkCli,
		topicAcl:  make(map[string]bool),
		genMsgID:  genMsgIdFunc,
	}
	go r.loop()
	return r, nil
}

func (r *replicator) Setup(producer *Producer) {
	r.producer = producer
	r.batch = NewBatchReplicator(r, r.batchSize)
}

// Test if the topic is allowed to be replicated across datacenters
func (r *replicator) Allow(topic string) bool {
	if topic == r.ReplicationTopic {
		// avoid replicating loop
		return false
	}
	return r.topicAcl[topic]
}

// Looping to update topic acl
func (r *replicator) loop() {
	for {
		if err := r.updateACL(); err != nil {
			log.ErrorLogger.WithFields(logrus.Fields{
				"err": err,
			}).Warn("update acl failed")
		}
		time.Sleep(r.SyncInterval)
	}
}

func (r *replicator) updateACL() error {
	topicList, _, err := r.zkCli.Children(ReplicationConfigDir)
	if err != nil {
		if err == zk.ErrNoNode {
			return util.ZKCreatePersistentPath(r.zkCli, ReplicationConfigDir, nil)
		}
		return err
	}

	acl := make(map[string]bool)
	for _, topic := range topicList {
		acl[topic] = true
	}
	r.topicAcl = acl // Lock is not needed
	return nil
}

func (r *replicator) Replicate(msg *ReplicatedMessage) (e error) {
	msg.SrcDatacenter = r.MyDatacenter // setup the origin datacenter
	metrics.Replication.Total.WithLabelValues(r.MyDatacenter).Inc()
	metrics.Replication.TopicTotal.WithLabelValues(r.MyDatacenter, msg.Topic).Inc()
	defer func() {
		if e != nil {
			metrics.Replication.Errors.WithLabelValues(r.MyDatacenter).Inc()
			metrics.Replication.TopicErrors.WithLabelValues(r.MyDatacenter, msg.Topic).Inc()
		}
	}()
	msg.MsgID = r.genMsgID()

	// To keep the messages order in the original (user's) partition, we should hash the origin
	// partitionID into replication partition.
	key := genReplicatedMsgKey(msg)
	partitionID, err := r.producer.SelectPartition(r.ReplicationTopic, key, "hash")
	if err != nil {
		return fmt.Errorf("failed to select partition: %s", err)
	}
	batchID, digestCh, err := r.batch.GetBatch(partitionID, key)
	if err != nil {
		return err
	}
	return r.batch.Send(partitionID, batchID, key, digestCh, msg)
}

func (r *replicator) Close() {
	r.batch.Close()
}

func genBatchId() string {
	return util.GenUniqueID()
}

// Generate replication message's key.
func genReplicatedMsgKey(msg *ReplicatedMessage) string {
	return fmt.Sprintf("%s:%d", msg.Key, msg.Partition)
}
