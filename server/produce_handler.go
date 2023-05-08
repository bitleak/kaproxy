package server

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"net/http"

	"github.com/bitleak/kaproxy/producer"
	"github.com/bitleak/kaproxy/util"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

func produceByPartitioner(c *gin.Context) {
	partitioner := c.DefaultPostForm("partitioner", "random")
	c.Params = append(c.Params, gin.Param{
		Key:   "partition",
		Value: partitioner,
	})
	produce(c)
}

func produceManual(c *gin.Context) {
	produce(c)
}

func produce(c *gin.Context) {
	logger := getLogger(c)

	topic := c.Param("topic")
	key := c.PostForm("key")
	value := c.PostForm("value")
	headers := c.PostForm("headers")
	var recordHeader []sarama.RecordHeader
	if headers != "" {
		err := json.Unmarshal([]byte(headers), &recordHeader)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"headers": headers,
				"err":     err,
			}).Error("Failed to parse headers")
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid headers"})
			return
		}
	}
	replicateFlag := c.DefaultQuery("replicate", "yes")

	partition, err := srv.producer.SelectPartition(topic, key, c.Param("partition"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var response *producer.ProduceResponse
	if replicateFlag == "yes" {
		response, err = srv.producer.ProduceMessageWithReplication(topic, key, value, recordHeader, partition, c.Param("partition"))
	} else {
		response, err = srv.producer.ProduceMessageWithoutReplication(topic, key, value, recordHeader, partition)
	}

	if err != nil {
		logger.WithFields(logrus.Fields{
			"topic": topic,
			"err":   err,
		}).Error("Failed to send message")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to send message"})
	} else {
		c.JSON(http.StatusOK, response)
	}
}

// Use multipart/form-data to produce many messages at a time
// In the multipart form, every form name is the `key`, and it's value
// is the message value.
//
// Notes:
// Even for a single message, using multipart/form-data instead of form-urlencoded is
// better, because urlencode a large payload is compute-intensive job, while multipart/form-data
// doesn't do any encoding.
//
// TODO: use SyncProducer.SendMessages API
func batchProduce(c *gin.Context) {
	logger := getLogger(c)

	partitioner := c.DefaultQuery("partitioner", "random")
	topic := c.Param("topic")
	replicateFlag := c.DefaultQuery("replicate", "yes")

	data, err := util.ParseMultipartForm(c.Request)
	if err != nil {
		logger.WithField("err", err).Warn("Failed to parse multipart form")
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid multipart form"})
		return
	}
	var msgHead [][]sarama.RecordHeader
	if h := c.Request.Header.Get("__headers__"); h != "" {
		if err := json.Unmarshal([]byte(h), &msgHead); err != nil {
			logger.WithFields(logrus.Fields{
				"__headers__": h,
				"topic":       topic,
				"err":         err,
			}).Error("Failed to parse __headers__")
			c.JSON(http.StatusBadRequest, gin.H{
				"error":       "invalid __headers__",
				"topic":       topic,
				"__headers__": h,
			})
			return
		}
		if len(msgHead) > 0 && len(msgHead) != len(data) {
			errMsg := "the length of the __headers__ must be equal to the length of the multipart form data"
			logger.WithFields(logrus.Fields{
				"__headers__": h,
			}).Warn(errMsg)
			c.JSON(http.StatusBadRequest, gin.H{
				"error":       errMsg,
				"topic":       topic,
				"__headers__": h,
			})
			return
		}
	}

	for i, v := range data { // key: v[0]; value: v[1]
		partition, err := srv.producer.SelectPartition(topic, v[0], partitioner)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":       err.Error(),
				"topic":       topic,
				"key":         v[0],
				"partitioner": partitioner,
			})
			return
		}
		var h []sarama.RecordHeader
		if len(msgHead) == len(data) {
			h = msgHead[i]
		}
		if replicateFlag == "yes" {
			_, err = srv.producer.ProduceMessageWithReplication(topic, v[0], v[1], h, partition, c.Param("partition"))
		} else {
			_, err = srv.producer.ProduceMessageWithoutReplication(topic, v[0], v[1], h, partition)
		}

		if err != nil {
			logger.WithFields(logrus.Fields{
				"topic": topic,
				"err":   err,
			}).Error("Failed to send message")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"msg": "succeeded", "count": len(data)})
	return
}
