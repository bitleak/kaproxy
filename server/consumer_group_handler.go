package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/bitleak/kaproxy/util"

	"github.com/bitleak/kaproxy/consumer"
	"github.com/meitu/go-zookeeper/zk"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

const (
	kaproxyOwner  = "kaproxy"
	consumerZKDir = "/consumers/"
)

func createConsumerGroup(c *gin.Context) {
	logger := getLogger(c)
	group := c.Param("group")
	topics := []string{}
	if c.PostForm("topics") != "" {
		topics = strings.Split(c.PostForm("topics"), ",")
	}
	metadata := consumer.GroupMetadata{
		Owner:     kaproxyOwner,
		Semantics: consumer.SemanticAtMostOnce,
		Consumer:  srv.conf.Consumer,
		Topics:    topics,
	}
	bytes, _ := json.Marshal(metadata)
	err := util.ZKCreatePersistentPath(srv.zkCli, consumerZKDir+group, bytes)
	if err == zk.ErrNodeExists {
		c.JSON(http.StatusBadRequest, gin.H{"err": "consumer group has already exists"})
		return
	} else if err != nil {
		logger.WithFields(logrus.Fields{
			"group": group,
			"err":   err,
		}).Error("Failed to get the metadata from ZK")
		c.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"group": group})
}

func deleteConsumerGroup(c *gin.Context) {
	logger := getLogger(c)
	group := c.Param("group")
	bytes, _, err := srv.zkCli.Get(consumerZKDir + group)
	if err == zk.ErrNoNode {
		c.JSON(http.StatusNotFound, gin.H{"err": "group was not found"})
		return
	} else if err != nil {
		logger.WithFields(logrus.Fields{
			"group": group,
			"err":   err,
		}).Error("Failed to get the metadata from ZK")
		c.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
		return
	}
	var metadata consumer.GroupMetadata
	if err := json.Unmarshal(bytes, &metadata); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"err": "consumer group with invalid metadata format"})
		return
	}
	if metadata.Owner != kaproxyOwner {
		c.JSON(http.StatusBadRequest, gin.H{"err": "the owner of the consumer group wasn't kaproxy"})
		return
	}
	if !metadata.Stopped {
		c.JSON(http.StatusBadRequest, gin.H{"err": "you should stop the consumer group first"})
		return
	}
	err = srv.zkCli.DeleteRecursive(consumerZKDir+group, -1)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"group": group,
			"err":   err,
		}).Error("Failed to get the metadata from ZK")
		c.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
		return
	}
	c.JSON(http.StatusOK, group)
}

func listConsumerGroups(c *gin.Context) {
	groups := srv.consumer.ListConsumerGroup()
	c.JSON(http.StatusOK, groups)
}

func setConsumerGroup(c *gin.Context) {
	var err error
	group := c.Param("group")
	action := c.Param("action")
	logger := getLogger(c)
	logger.WithFields(logrus.Fields{
		"group":  group,
		"action": action,
	}).Info("Administrate consumer group")
	switch action {
	case "start":
		err = srv.consumer.StartConsumerGroup(group)
	case "stop":
		err = srv.consumer.StopConsumerGroup(group)
	case "export":
		exportConsumerGroup(c)
		return
	case "state":
		getConsumerGroupState(c)
		return
	default:
		err = fmt.Errorf("unknown action %s", action)
	}
	if err != nil {
		logger.Warn("Failed to administrate consumer group")
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	} else {
		c.JSON(http.StatusOK, gin.H{"result": "success"})
	}
}

func exportConsumerGroup(c *gin.Context) {
	group := c.Param("group")
	info, err := srv.consumer.Export(group)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	} else {
		c.JSON(http.StatusOK, info)
	}
}

func getConsumerGroupState(c *gin.Context) {
	group := c.Param("group")
	state, err := srv.consumer.GetConsumerGroupState(group)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "consumer group not found"})
		return
	} else {
		c.JSON(http.StatusOK, gin.H{"state": state})
		return
	}
}
