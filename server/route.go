package server

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/meitu/kaproxy/metrics"
)

func filterTopicReq(c *gin.Context) {
	token := c.Query("token")
	if token == "" {
		token = c.PostForm("token")
	}
	topic := c.Param("topic")
	valid := srv.tokenManager.IsAllowProduce(token, topic)
	if !valid {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": fmt.Sprintf("app [%s] wasn't allowed to access topic [%s]",
				token, topic),
		})
		// abort other handlers
		c.Abort()
	}
}

func filterGroupReq(c *gin.Context) {
	token := c.PostForm("token")
	if token == "" {
		token = c.Query("token")
	}
	group := c.Param("group")
	valid := srv.tokenManager.IsAllowConsume(token, group)
	if !valid {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": fmt.Sprintf("app [%s] wasn't allowed to access group [%s]",
				token, group),
		})

		c.Abort()
	}
}

func setupProxyRouters(engine *gin.Engine) {
	// consume api
	consumeAPI := engine.Group("/group/:group")
	{
		consumeAPI.Use(filterGroupReq)
		consumeAPI.GET("/topic/:topic", metrics.TrackHttpMetrics("consume"), consume)
		consumeAPI.POST("/topic/:topic/ack", ackConsume)
	}
	// produce api
	produceAPI := engine.Group("/topic/:topic")
	{
		produceAPI.Use(filterTopicReq)
		produceAPI.POST("", metrics.TrackHttpMetrics("produce"), produceByPartitioner)
		produceAPI.POST("/partition/:partition", metrics.TrackHttpMetrics("produce"), produceManual)
		produceAPI.POST("/batch", metrics.TrackHttpMetrics("batch_produce"), batchProduce)
	}
}
