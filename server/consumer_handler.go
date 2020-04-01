package server

import (
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/Shopify/sarama"
	"github.com/gin-gonic/gin"
	"github.com/meitu/kaproxy/consumer"
	"github.com/meitu/kaproxy/log"
	"github.com/sirupsen/logrus"
)

func reply(c *gin.Context, message *sarama.ConsumerMessage) {
	if message == nil {
		c.JSON(http.StatusNoContent, nil)
		return
	}
	if c.Query("source") == consumer.PeerProxyID {
		c.JSON(http.StatusOK, message)
		return
	}
	if utf8.Valid(message.Value) {
		c.JSON(http.StatusOK, gin.H{
			"key":       string(message.Key),
			"value":     string(message.Value),
			"topic":     message.Topic,
			"partition": message.Partition,
			"offset":    message.Offset,
			"encoding":  "utf8",
		})
	} else {
		c.JSON(http.StatusOK, gin.H{
			"key":       string(message.Key),
			"value":     message.Value,
			"topic":     message.Topic,
			"partition": message.Partition,
			"offset":    message.Offset,
			"encoding":  "base64",
		})
	}
}

func consume(c *gin.Context) {
	group := c.Param("group")
	topic := c.Param("topic")
	token := c.Query("token")
	source := c.Query("source")
	logger := getLogger(c)

	timeoutStr := c.DefaultQuery("timeout", "0")
	timeout, err := strconv.ParseUint(timeoutStr, 10, 64)
	if err != nil || timeout < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid timeout"})
		return
	}
	if timeout < 3 {
		timeout = 3
	}

	ttrStr := c.DefaultQuery("ttr", "300000")
	ttr, err := strconv.ParseUint(ttrStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid ttr"})
		return
	}

	//consume local first, block 3ms
	message, err := srv.consumer.Consume(group, topic, 3*time.Millisecond,
		time.Duration(ttr)*time.Millisecond)
	if err == nil {
		reply(c, message)
		return
	}
	if err != consumer.ErrNoMessage && err != consumer.ErrNoPartition {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	timeout -= 3

	//if there is no message in local, pull from peers
	noPartition := err == consumer.ErrNoPartition
	if source != consumer.PeerProxyID {
		pullTimeout := timeout
		if !noPartition {
			pullTimeout = 10
		}
		if pullTimeout < 10 {
			pullTimeout = 10
		}
		message, err = srv.consumer.Puller.PullFromPeers(group, topic, token, pullTimeout, ttr, noPartition)
		if err == nil {
			reply(c, message)
			return
		}
		if err != consumer.ErrNoMessage && err != consumer.ErrNoPeer {
			logger.WithFields(logrus.Fields{
				"source":       source,
				"token":        token,
				"group":        group,
				"topic":        topic,
				"err":          err,
				log.UpgradeKey: "pullFromPeersFailed",
			}).Warn("Failed to fetch message from peer")
		}
	}

	//if no message in peers, consume in local and block
	blockTimeout := timeout
	//subtract the time spent by pull from peer
	if err != consumer.ErrNoPeer {
		blockTimeout -= 10
		if noPartition {
			blockTimeout = 0
		}
	}
	if blockTimeout < 3 {
		blockTimeout = 3
	}

	message, err = srv.consumer.Consume(group, topic, time.Duration(blockTimeout)*time.Millisecond,
		time.Duration(ttr)*time.Millisecond)
	if err != nil {
		c.JSON(http.StatusNoContent, nil)
		return
	}
	reply(c, message)
}

func ackConsume(c *gin.Context) {
	group := c.Param("group")
	topic := c.Param("topic")
	token := c.Query("token")
	partition, err := strconv.ParseUint(c.PostForm("partition"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid partition"})
		return
	}
	offset, err := strconv.ParseUint(c.PostForm("offset"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid offset"})
		return
	}

	err = srv.consumer.ACK(token, group, topic, int32(partition), int64(offset))
	if err != nil {
		if err == consumer.ErrGroupNotFound || err == consumer.ErrTopicNotFound || err == consumer.ErrGroupStopped {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		} else if err == consumer.ErrUnackMessageNotFound {
			c.JSON(http.StatusMovedPermanently, gin.H{"error": err.Error()})
		} else if err == consumer.ErrGroupNotAllowACK {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"msg": "succeeded"})
}
