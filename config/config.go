package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/Shopify/sarama"
)

// Config for the proxy
type Config struct {
	Server      ServerConfig
	Kafka       KafkaConfig
	Consumer    ConsumerConfig
	Producer    ProducerConfig
	Replication ReplicationConfig
}

func newConfig() *Config {
	conf := new(Config)

	conf.Server.AccessLogEnable = true
	conf.Server.Host = "0.0.0.0"
	conf.Server.Port = 8080
	conf.Server.AdminHost = "0.0.0.0"
	conf.Server.AdminPort = 8081
	conf.Server.LogDir = "."

	conf.Kafka.Brokers = []string{"127.0.0.1:9092"}
	conf.Kafka.Version.Version = sarama.MinVersion
	conf.Kafka.Zookeepers = []string{"127.0.0.1:2181"}
	conf.Kafka.ZKSessionTimeout.Duration = 6 * time.Second

	conf.Consumer.OffsetAutoCommitInterval.Duration = 10 * time.Second
	conf.Consumer.OffsetAutoReset.Reset = sarama.OffsetOldest
	conf.Consumer.ClaimPartitionRetryInterval.Duration = 2 * time.Second
	conf.Consumer.ChannelBufferSize = 4096
	conf.Consumer.UnackManagerSize = 32
	conf.Consumer.FetchDefault = 1048576
	conf.Consumer.MaxWaitTime.Duration = 250 * time.Millisecond
	conf.Consumer.MaxProcessingTime.Duration = 100 * time.Millisecond

	conf.Producer.Clients = 5
	conf.Producer.MaxMessageBytes = 1024 * 1024

	conf.Replication.BatchChecksumSize = 20
	conf.Replication.CrossDatacenterReplicationTopic = "__cross_datacenter_replication"
	conf.Replication.ReplicationAclUpdateInterval.Duration = 10 * time.Second

	return conf
}

func Load(path string) (*Config, error) {
	_, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	conf := newConfig()
	if _, err := toml.DecodeFile(path, conf); err != nil {
		return nil, err
	}

	conf.Consumer.AutoReconnectInterval.Duration = conf.Kafka.ZKSessionTimeout.Duration / 3

	if conf.Replication.MyDatacenterName == "" {
		return nil, errors.New("datacenter name must not be empty")
	}

	return conf, nil
}

type ServerConfig struct {
	Host            string
	Port            int
	AdminHost       string
	AdminPort       int
	LogDir          string
	AccessLogEnable bool
}

type KafkaConfig struct {
	Brokers          []string
	Version          version
	Zookeepers       []string
	ZKSessionTimeout duration
	ZKChroot         string
}

type ProducerConfig struct {
	Clients         int
	MaxMessageBytes int
}

type ConsumerConfig struct {
	OffsetAutoCommitInterval    duration `json:"offset_auto_commit_interval"`
	OffsetAutoReset             reset    `json:"offset_auto_reset"`
	ClaimPartitionRetryInterval duration `json:"claim_partition_retry_interval"`
	AutoReconnectInterval       duration `json:"auto_reconnect_interval"`
	UnackManagerSize            int      `json:"unack_manager_size"`
	//sarama config
	ChannelBufferSize int      `json:"channel_buffer_size"`
	FetchDefault      int32    `json:"fetch_default"`
	MaxWaitTime       duration `json:"max_wait_time"`
	MaxProcessingTime duration `json:"max_processing_time"`
}

type ReplicationConfig struct {
	MyDatacenterName                string
	CrossDatacenterReplicationTopic string
	ReplicationAclUpdateInterval    duration
	BatchChecksumSize               int
	UUIDServers                     []string
}

type duration struct {
	time.Duration
}

func (d duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

func (d *duration) UnmarshalJSON(b []byte) error {
	var err error
	var s string
	if err = json.Unmarshal(b, &s); err != nil {
		return err
	}
	d.Duration, err = time.ParseDuration(s)
	return err
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func (d *duration) MarshalText() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}

type reset struct {
	Reset int64
}

func (r reset) MarshalJSON() ([]byte, error) {
	switch r.Reset {
	case sarama.OffsetOldest:
		return json.Marshal("oldest")
	case sarama.OffsetNewest:
		return json.Marshal("newest")
	default:
		return nil, errors.New("invalid offset auto reset configuration")
	}
}

func (r *reset) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	switch s {
	case "oldest":
		r.Reset = sarama.OffsetOldest
	case "newest":
		r.Reset = sarama.OffsetNewest
	default:
		return errors.New("offset auto reset must be oldest or newest")
	}
	return nil
}

func (r *reset) UnmarshalText(text []byte) error {
	switch string(text) {
	case "oldest":
		r.Reset = sarama.OffsetOldest
	case "newest":
		r.Reset = sarama.OffsetNewest
	default:
		return errors.New("offset auto reset must be oldest or newest")
	}
	return nil
}

func (r *reset) MarshalText() ([]byte, error) {
	switch r.Reset {
	case sarama.OffsetOldest:
		return []byte("oldest"), nil
	case sarama.OffsetNewest:
		return []byte("newest"), nil
	default:
		return nil, errors.New("invalid offset auto reset configuration")
	}
}

type version struct {
	Version sarama.KafkaVersion
}

func (v *version) UnmarshalText(text []byte) (err error) {
	v.Version, err = sarama.ParseKafkaVersion(string(text))
	if err != nil {
		return err
	}
	supported := false
	for _, sv := range sarama.SupportedVersions {
		if v.Version.String() == sv.String() {
			supported = true
		}
	}
	if !supported {
		return fmt.Errorf("invalid version %s ", string(text))
	}
	return nil
}
