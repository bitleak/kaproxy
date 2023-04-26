package producer

import (
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"math/rand"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bitleak/kaproxy/config"
)

type ProduceResponse struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

type Producer struct {
	saramaClients   []sarama.Client
	saramaProducers []sarama.SyncProducer
	replicator      Replicator
	clients         int
}

func NewProducer(conf *config.Config) (*Producer, error) {
	var err error
	p := new(Producer)
	p.clients = conf.Producer.Clients
	p.saramaClients = make([]sarama.Client, p.clients)
	p.saramaProducers = make([]sarama.SyncProducer, p.clients)
	produceConf := sarama.NewConfig()
	produceConf.Producer.Return.Successes = true
	produceConf.Producer.Partitioner = sarama.NewManualPartitioner
	produceConf.Producer.MaxMessageBytes = conf.Producer.MaxMessageBytes
	produceConf.Net.DialTimeout = 3100 * time.Millisecond
	produceConf.Net.ReadTimeout = 10 * time.Second
	produceConf.Net.WriteTimeout = 3 * time.Second
	produceConf.Version = conf.Kafka.Version.Version

	for i := 0; i < p.clients; i++ {
		p.saramaClients[i], err = sarama.NewClient(conf.Kafka.Brokers, produceConf)
		if err != nil {
			return nil, fmt.Errorf("failed to init sarama client: %s", err)
		}
		p.saramaProducers[i], err = sarama.NewSyncProducerFromClient(p.saramaClients[i])
		if err != nil {
			return nil, fmt.Errorf("failed to init sarama producer: %s", err)
		}
	}

	p.replicator, err = NewReplicator(
		&conf.Replication,
		&conf.Kafka,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to init replicator: %s", err)
	}
	p.replicator.Setup(p)

	return p, nil
}

func GetFnvHash(key string) int32 {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return int32(hash.Sum32() >> 1) // Right shit to avoid converting to negative value
}

func (p *Producer) ProduceMessageWithoutReplication(topic, key, value string, recordHeader []sarama.RecordHeader, partition int32) (*ProduceResponse, error) {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.StringEncoder(value),
		Partition: partition,
		Headers:   recordHeader,
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}

	partition, offset, err := p.getProducer(topic).SendMessage(msg)
	if err != nil {
		return nil, err
	}
	response := &ProduceResponse{
		Partition: partition,
		Offset:    offset,
	}
	return response, nil
}

func (p *Producer) ProduceMessageWithReplication(topic, key, value string, recordHeader []sarama.RecordHeader, partition int32, partitionMethod string) (*ProduceResponse, error) {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.StringEncoder(value),
		Partition: partition,
		Headers:   recordHeader,
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}

	partition, offset, err := p.getProducer(topic).SendMessage(msg)
	if err != nil {
		return nil, err
	} else if p.replicator.Allow(topic) {
		replicatedMsg := ReplicatedMessage{
			Topic:           topic,
			Key:             []byte(key),
			Value:           []byte(value),
			Headers:         recordHeader,
			Partition:       partition,
			PartitionMethod: partitionMethod,
		}
		if err := p.replicator.Replicate(&replicatedMsg); err != nil {
			return nil, err
		}
	}
	response := &ProduceResponse{
		Partition: partition,
		Offset:    offset,
	}
	return response, nil
}

func (p *Producer) GetNumPartitions(topic string) (int32, error) {
	partitions, err := p.getCli(topic).Partitions(topic)
	if err != nil {
		return -1, err
	}
	return int32(len(partitions)), nil
}

func (p *Producer) SelectPartition(topic, key, partitionMethod string) (partition int32, err error) {
	numPartitions, err := p.GetNumPartitions(topic)
	if err != nil {
		return -1, err
	}

	switch partitionMethod {
	case "hash":
		if key == "" {
			return -1, errHashWithoutKey
		}
		partition = GetFnvHash(key) % numPartitions
	case "random":
		partition = rand.Int31n(numPartitions)
	default:
		n, err := strconv.ParseInt(partitionMethod, 10, 32)
		if err != nil {
			return -1, errInvalidPartitionType
		}
		partition = int32(n)
		if partition >= numPartitions || partition < 0 {
			return -1, errInvalidPartitionIndex
		}
	}
	return partition, nil
}

func (p *Producer) Close() {
	p.replicator.Close()
}

func (p *Producer) getCli(topic string) sarama.Client {
	return p.saramaClients[p.hashByTopic(topic)]
}

func (p *Producer) getProducer(topic string) sarama.SyncProducer {
	return p.saramaProducers[p.hashByTopic(topic)]
}

func (p *Producer) hashByTopic(topic string) int {
	return int(crc32.ChecksumIEEE([]byte(topic))) % p.clients
}
