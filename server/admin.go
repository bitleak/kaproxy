package server

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"runtime/debug"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/meitu/kaproxy/log"
	"github.com/meitu/kaproxy/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

type serviceStatus struct {
	httpCode int
	lock     sync.RWMutex
}

var (
	srvStatus serviceStatus
	gcStats   *debug.GCStats
	gcLock    sync.RWMutex
)

func handleStatus(c *gin.Context) {
	srvStatus.lock.RLock()
	defer srvStatus.lock.RUnlock()
	c.JSON(srvStatus.httpCode, nil)
}

func handleSetStatus(c *gin.Context) {
	srvStatus.lock.Lock()
	defer srvStatus.lock.Unlock()

	status := c.Query("status")
	switch status {
	case "disable":
		srvStatus.httpCode = http.StatusServiceUnavailable
		log.Logger.Info("service is offline manually")
		c.JSON(http.StatusOK, gin.H{"msg": "service is offline manually"})
	case "enable":
		srvStatus.httpCode = http.StatusOK
		log.Logger.Info("service is running")
		c.JSON(http.StatusOK, gin.H{"msg": "service is running"})
	default:
		c.JSON(http.StatusOK, gin.H{"error": "invalid status"})
	}
}

func showVersion(c *gin.Context) {
	c.JSON(http.StatusOK,
		gin.H{
			"version":      Version,
			"build_date":   BuildDate,
			"build_commit": BuildCommit,
		})
}

func handleLogLevel(c *gin.Context) {
	switch c.Request.Method {
	case "GET":
		c.JSON(http.StatusOK, log.Logger.Level.String())

	case "POST":
		newLevel, err := logrus.ParseLevel(c.PostForm("loglevel"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		} else {
			log.Logger.Level = newLevel
			msg := fmt.Sprintf("set loglevel to %s succ", newLevel.String())
			c.JSON(http.StatusOK, gin.H{"msg": msg})
		}

	default:
		c.JSON(http.StatusMethodNotAllowed, nil)
	}
}

func collectGCMetrics() {
	gcLock.Lock()
	defer gcLock.Unlock()
	newGCStats := &debug.GCStats{}
	debug.ReadGCStats(newGCStats)
	count := newGCStats.NumGC
	if gcStats != nil {
		count -= gcStats.NumGC
	}
	metrics.GC.Num.Add(float64(count))
	n := len(newGCStats.Pause)
	for i := 0; i < int(count) && i < n; i++ {
		metrics.GC.Duration.Observe(float64(newGCStats.Pause[i] / time.Millisecond))
	}
	gcStats = newGCStats
}

func handlePrometheusMetrics(c *gin.Context) {
	// Collect gc metrics
	collectGCMetrics()
	promhttp.Handler().ServeHTTP(c.Writer, c.Request)
}

func listConsumerGroup(c *gin.Context) {
	groups := srv.consumer.ListConsumerGroup()
	c.JSON(http.StatusOK, groups)
}

func handleGroup(c *gin.Context) {
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

func handlePProf(ctx *gin.Context) {
	metric := ctx.Param("metric")
	switch metric {
	case "heap", "goroutine", "block", "threadcreate", "mutex":
		pprof.Handler(metric).ServeHTTP(ctx.Writer, ctx.Request)
	case "cmdline":
		pprof.Cmdline(ctx.Writer, ctx.Request)
	case "profile":
		pprof.Profile(ctx.Writer, ctx.Request)
	case "symbol":
		pprof.Symbol(ctx.Writer, ctx.Request)
	case "trace":
		pprof.Trace(ctx.Writer, ctx.Request)
	default:
		pprof.Index(ctx.Writer, ctx.Request)
	}
}

func setupAdminRouter(engine *gin.Engine) {
	devops := engine.Group("devops")
	{
		devops.GET("/version", showVersion)
		devops.GET("/status", handleStatus)
	}
	log := engine.Group("log")
	{
		log.GET("/level", handleLogLevel)
		log.POST("/level", handleLogLevel)
	}
	group := engine.Group("group")
	{
		group.GET("", listConsumerGroup)
		group.GET("/", listConsumerGroup)
		group.POST("/:group/:action", handleGroup)
		group.GET("/:group/state", getConsumerGroupState)
		group.GET("/:group/export", exportConsumerGroup)
	}
	tokens := engine.Group("tokens")
	{
		tokens.GET("", listToken)
		tokens.POST("", createToken)
		tokens.GET("/:token", getToken)
		tokens.PATCH("/:token/:action", updateToken)
		tokens.DELETE("/:token", deteteToken)
	}
	engine.GET("/pprof/:metric", handlePProf)
	engine.GET("/metrics", handlePrometheusMetrics)
}

func init() {
	srvStatus.httpCode = http.StatusServiceUnavailable
}
