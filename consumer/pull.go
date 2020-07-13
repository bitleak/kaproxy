package consumer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bitleak/kaproxy/log"
	"github.com/bitleak/kaproxy/metrics"
	"github.com/bitleak/kaproxy/util"
	"github.com/sirupsen/logrus"
)

/*
So the peer must be in one of the following three stages:
1. Try to pull msg if the peer wasn't frozen, and add the peer into freezes while
   failed to pull the msg
2. Skip the peer if  it was during the freeze period
3. Randomly choose the requests to try to pull the msg before the frozen time
   was expired, avoids too many pulls at the same time when the frozen time was reached
*/

const (
	proxyIDDir  = "/proxy/ids"
	proxyIDPath = proxyIDDir + "/%s"
	PeerProxyID = "__peer_proxy__"
)

const (
	freezeTime = 200 * time.Millisecond
	probeTime  = -10 * time.Millisecond
)

type Puller struct {
	addr         string
	aliveProxies map[string]struct{}
	httpClient   http.Client
	cluster      *Consumer
	stopper      chan struct{}
	weights      sync.Map // Key is string which format is {group}-{topic}, value is weightsList
	freezer      sync.Map
}

func newPuller(cluster *Consumer) (*Puller, error) {
	addr := getAddrFromProxyID(cluster.proxyID)
	transport := http.Transport{
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, 2*time.Second)
		},
	}
	httpClient := http.Client{Transport: &transport}
	puller := &Puller{
		addr:         addr,
		aliveProxies: make(map[string]struct{}),
		httpClient:   httpClient,
		cluster:      cluster,
	}
	return puller, nil
}

func (p *Puller) Start() error {
	util.ZKCreatePersistentPath(p.cluster.zkCli, proxyIDDir, nil)

	p.stopper = make(chan struct{})
	go p.watchProxyListLoop()
	go p.loop(5 * time.Second)
	zkPath := fmt.Sprintf(proxyIDPath, p.addr)
	return util.ZKCreateEphemeralPath(p.cluster.zkCli, zkPath, nil)
}

func (p *Puller) Close() error {
	close(p.stopper)
	zkPath := fmt.Sprintf(proxyIDPath, p.addr)
	return p.cluster.zkCli.Delete(zkPath, -1)
}

func (p *Puller) watchProxyListLoop() {
	zkClient := p.cluster.zkCli
	for {
		proxyList, _, watcher, err := zkClient.ChildrenW(proxyIDDir)
		if err != nil {
			log.ErrorLogger.Errorf("Failed to watch proxy list, %s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		newAliveProxies := make(map[string]struct{})
		for _, proxy := range proxyList {
			newAliveProxies[proxy] = struct{}{}
		}
		p.aliveProxies = newAliveProxies
		select {
		case <-p.stopper:
			return
		case <-watcher.EvCh:
			log.ErrorLogger.Info("Proxy list change")
		}
	}
}

func (p *Puller) PullFromPeers(group, topic, token string, timeout, ttr uint64, noPartition bool) (*sarama.ConsumerMessage, error) {
	peer := p.pickPeer(group, topic, noPartition)
	if peer == "" {
		return nil, ErrNoPeer
	}
	url := fmt.Sprintf("http://%s/group/%s/topic/%s?token=%s&source=%s&ttr=%d&timeout=%d",
		peer, group, topic, token, PeerProxyID, ttr, timeout)
	resp, err := p.httpClient.Get(url)
	if err != nil {
		p.freeze(peer, group, topic)
		return nil, err
	}
	defer resp.Body.Close()

	metrics.Http.PullFromPeer.WithLabelValues(token, group, topic, peer, strconv.Itoa(resp.StatusCode)).Inc()
	if resp.StatusCode == http.StatusNoContent {
		ioutil.ReadAll(resp.Body)
		p.freeze(peer, group, topic)
		return nil, ErrNoMessage
	}
	if resp.StatusCode != http.StatusOK {
		ioutil.ReadAll(resp.Body)
		p.freeze(peer, group, topic)
		return nil, fmt.Errorf("incorrent http status code: %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		resp.Body.Close()
		return nil, err
	}
	var msg sarama.ConsumerMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

func (p *Puller) loop(interval time.Duration) {
	defer util.WithRecover(log.ErrorLogger)
	ticker := time.NewTicker(interval)
	for {
		<-ticker.C
		groups := p.cluster.ListConsumerGroup()
		for _, group := range groups {
			metadata := p.cluster.GetMetadataByName(group)
			if metadata == nil {
				continue //maybe group was deleted after listing groups, continue
			}
			topics := metadata.Topics
			for _, topic := range topics {
				p.updateWeights(group, topic)
			}
		}
	}
}

func (p *Puller) updateWeights(group, topic string) {
	var topicWeights weightsList
	zkClient := p.cluster.zkCli
	zkPath := fmt.Sprintf(weightPath, group, topic)
	weightsBytes, _, err := zkClient.Get(zkPath)
	if err != nil {
		log.ErrorLogger.WithFields(logrus.Fields{
			"group": group,
			"topic": topic,
			"err":   err,
		}).Warn("Failed to get weight from zk")
		return
	}
	err = json.Unmarshal(weightsBytes, &topicWeights)
	if err != nil {
		log.ErrorLogger.WithFields(logrus.Fields{
			"group": group,
			"topic": topic,
			"err":   err,
		}).Warn("Failed to unmarshal weights bytes")
		return
	}
	for i, weight := range topicWeights {
		if weight.Addr == p.addr {
			topicWeights = append(topicWeights[:i], topicWeights[i+1:]...)
			break
		}
	}
	key := fmt.Sprintf("{%s}-{%s}", group, topic)
	p.weights.Store(key, topicWeights)
}

func (p *Puller) pickPeer(group, topic string, noPartition bool) string {
	key := fmt.Sprintf("{%s}-{%s}", group, topic)
	value, ok := p.weights.Load(key)
	if !ok {
		return ""
	}

	peers := value.(weightsList)
	peerWithLags := make(weightsList, 0)
	total := float32(0)
	for _, peer := range peers {
		if peer.Weight == 0 {
			continue
		}
		if p.isPeerPollable(peer.Addr, group, topic) {
			peerWithLags = append(peerWithLags, peer)
			total += peer.Weight
		}
	}

	//pick peer by weight
	if len(peerWithLags) != 0 {
		r := rand.Float32()
		for _, peer := range peerWithLags {
			if r < peer.Weight/total {
				return peer.Addr
			}
			r -= peer.Weight / total
		}
	}

	//if this proxy claim no partition, randomly pick a peer that claim a partition
	if noPartition && len(peers) > 0 {
		return peers[rand.Intn(len(peers))].Addr
	}
	return ""
}

func (p *Puller) isPeerPollable(peer, group, topic string) bool {
	_, ok := p.aliveProxies[peer]
	if !ok {
		return false
	}
	key := p.freezeKey(peer, group, topic)
	value, ok := p.freezer.Load(key)
	if !ok {
		return true
	}
	timestamp := value.(time.Time)
	now := time.Now()
	if now.After(timestamp) {
		p.freezer.Delete(key)
		return true
	}
	if now.After(timestamp.Add(probeTime)) && rand.Int31n(10) < 1 {
		return true
	}
	return false
}

func (p *Puller) freeze(peer, group, topic string) {
	key := p.freezeKey(peer, group, topic)
	p.freezer.Store(key, time.Now().Add(freezeTime))
}

func (p *Puller) freezeKey(peer, group, topic string) string {
	return fmt.Sprintf("{%s}-{%s}-{%s}", group, topic, peer)
}

func getAddrFromProxyID(proxyID string) string {
	arr := strings.Split(proxyID, "}")
	return strings.TrimPrefix(arr[0], "{")
}

type weightsList []*proxyWeight

func (w weightsList) Len() int {
	return len(w)
}

func (w weightsList) Swap(i, j int) {
	w[i], w[j] = w[j], w[i]
}

func (w weightsList) Less(i, j int) bool {
	return w[i].Weight > w[j].Weight
}
