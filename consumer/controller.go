package consumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bitleak/kaproxy/log"
	"github.com/bitleak/kaproxy/util"
	"github.com/meitu/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
)

type proxyWeight struct {
	Addr   string  `json:"addr"`
	Weight float32 `json:"weight"`
}

type controller struct {
	cluster *Consumer
	stopper chan struct{}
}

func newController(cluster *Consumer) *controller {
	return &controller{
		cluster: cluster,
		stopper: make(chan struct{}),
	}
}

func (c *controller) start() {
	defer util.WithRecover(log.ErrorLogger)
	zkClient := c.cluster.zkCli
	proxyID := c.cluster.proxyID
	for {
		select {
		case <-c.stopper:
			return
		default:
		}

		err := util.ZKCreateEphemeralPath(zkClient, controllerPath, []byte(proxyID))
		if err != nil && err != zk.ErrNodeExists {
			log.ErrorLogger.Errorf("Failed to create controller path in ZK: %s", err)
			return
		}
		controller, _, watcher, err := zkClient.GetW(controllerPath)
		if err != nil {
			log.ErrorLogger.Errorf("Failed to watch controller path in ZK: %s", err)
			return
		}

		if bytes.Compare(controller, []byte(proxyID)) == 0 {
			log.ErrorLogger.Infof("The proxy[%s] would become controller", string(controller))
			c.masterLoop(watcher.EvCh)
		} else {
			log.ErrorLogger.Infof("The proxy[%s] would become slave", string(controller))
			c.slaveLoop(watcher.EvCh)
		}
	}
}

func (c *controller) stop() error {
	close(c.stopper)
	return c.cluster.zkCli.Delete(controllerPath, -1)
}

func (c *controller) masterLoop(ch <-chan zk.Event) {
	c.calcAndUpdateGroupWeights()
	//TODO: configure sleep time
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ch:
			log.ErrorLogger.Info("the controller would be exited, as event changed was received")
			return
		case <-ticker.C:
			c.calcAndUpdateGroupWeights()
		case <-c.stopper:
			log.ErrorLogger.Info("the controller would be exited, as the stop singal was received")
			return
		}
	}
}

func (c *controller) slaveLoop(ch <-chan zk.Event) {
	select {
	case <-ch:
		log.ErrorLogger.Info("the slave would be exited, as event changed was received")
	case <-c.stopper:
		log.ErrorLogger.Info("the slave would be exited, as the stop singal was received")
	}
}

func (c *controller) calcAndUpdateGroupWeights() {
	groups := c.cluster.ListConsumerGroup()
	for _, group := range groups {
		cg := c.cluster.GetConsumerGroup(group)
		// we would stop update the weight when the consumer group
		// was deleted or stopped by administrator, and the metadata was
		// empty means this consumer group wasn't created by proxy.
		if cg == nil || cg.IsStopped() || cg.metadata == nil {
			continue
		}
		metadata := cg.metadata
		topics := make([]string, len(metadata.Topics))
		copy(topics, metadata.Topics)
		for _, topic := range topics {
			c.calcAndUpdateTopicWeights(group, topic)
		}
	}
}

func (c *controller) calcAndUpdateTopicWeights(group, topic string) {

	proxyStat, total := c.getLagsGroupByProxy(group, topic)
	proxyWeights := make([]proxyWeight, 0)

	for addr, lag := range proxyStat {
		var weight float32
		if lag == 0 {
			weight = 0
		} else {
			weight = float32(lag) / float32(total)
		}
		proxyWeights = append(proxyWeights, proxyWeight{
			Addr:   addr,
			Weight: weight,
		})
	}
	err := c.writeProxyWeightsToZK(group, topic, proxyWeights)
	if err != nil {
		log.ErrorLogger.WithFields(logrus.Fields{
			"group": group,
			"topic": topic,
			"err":   err,
		}).Warn("Failed to write proxy weights to zk")
	}
}

func (c *controller) getLagsGroupByProxy(group, topic string) (map[string]int64, int64) {
	total := int64(0)
	saramaClient := c.cluster.saramaClient
	zkClient := c.cluster.zkCli
	proxyStat := make(map[string]int64)

	partitions, err := saramaClient.Partitions(topic)
	if err != nil {
		log.ErrorLogger.WithFields(logrus.Fields{
			"group": group,
			"topic": topic,
			"err":   err,
		}).Error("Failed to get partitions from brokers")
		return nil, 0
	}

	for _, partID := range partitions {
		offset, _, err := getConsumerOffset(zkClient, group, topic, partID)
		if err != nil && err != zk.ErrNoNode {
			log.ErrorLogger.WithFields(logrus.Fields{
				"group":     group,
				"topic":     topic,
				"partition": partID,
				"err":       err,
			}).Debug("Failed to get offset")
			continue
		}
		if err == zk.ErrNoNode {
			offset = 0
		}
		logSize, err := saramaClient.GetOffset(topic, partID, sarama.OffsetNewest)
		if err != nil {
			log.ErrorLogger.WithFields(logrus.Fields{
				"group":     group,
				"topic":     topic,
				"partition": partID,
				"err":       err,
			}).Debug("Failed to get log size")
			continue
		}
		lag := logSize - offset
		owner, err := getConsumerOwner(zkClient, group, topic, partID)
		if err != nil {
			log.ErrorLogger.WithFields(logrus.Fields{
				"group":     group,
				"topic":     topic,
				"partition": partID,
				"err":       err,
			}).Debug("Failed to get owner")
			continue
		}
		//If you mix different consumption methods, you will get an owner that does not meet the specifications.
		if match, _ := regexp.MatchString(`\{.+\}-\{.+:\d+\}-\{\d+\}`, owner); !match {
			continue
		}
		addr := getAddrFromOwner(owner)
		if _, ok := proxyStat[addr]; !ok {
			proxyStat[addr] = lag
		} else {
			proxyStat[addr] += lag
		}
		total += lag
	}
	return proxyStat, total
}

func (c *controller) writeProxyWeightsToZK(group, topic string, proxyWeights []proxyWeight) error {
	zkPath := fmt.Sprintf(weightPath, group, topic)
	weightBytes, _ := json.Marshal(&proxyWeights)
	return util.ZKSetPersistentPath(c.cluster.zkCli, zkPath, weightBytes)
}

func getAddrFromOwner(owner string) string {
	arr := strings.Split(owner, "}")
	return strings.TrimPrefix(arr[1], "-{")
}
