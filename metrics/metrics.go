package metrics

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "infra"
	subsystem = "kaproxy"
)

var (
	Http        HttpMetrics
	GC          GCMetrics
	Batch       BatchMetrics
	Replication ReplicationMetrics
	Topic       *prometheus.CounterVec
)

type HttpMetrics struct {
	Latencies    *prometheus.HistogramVec
	Bandwidth    *prometheus.CounterVec
	Code         *prometheus.CounterVec
	PullFromPeer *prometheus.CounterVec
}

type GCMetrics struct {
	Num      prometheus.Counter
	Duration prometheus.Histogram
}

type BatchMetrics struct {
	Start prometheus.Counter
	Stop  prometheus.Counter
}

type ReplicationMetrics struct {
	Total       *prometheus.CounterVec
	Errors      *prometheus.CounterVec
	TopicTotal  *prometheus.CounterVec
	TopicErrors *prometheus.CounterVec
}

func Init() {
	Http.Latencies = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "http_latency_milliseconds",
			Help:      "rest api latencies",
			Buckets:   []float64{1, 5, 10, 20, 50, 100, 500, 1000, 5000},
		},
		[]string{"token", "group", "topic", "api"},
	)
	prometheus.MustRegister(Http.Latencies)
	Http.Bandwidth = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "http_bandwidth_bytes",
			Help:      "rest api bytes",
		},
		[]string{"token", "direction"},
	)
	prometheus.MustRegister(Http.Bandwidth)
	Http.Code = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "http_code",
			Help:      "reset api http code",
		},
		[]string{"token", "group", "topic", "api", "code"},
	)
	prometheus.MustRegister(Http.Code)
	Http.PullFromPeer = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "pull_from_peer",
			Help:      "pull message from peer",
		},
		[]string{"token", "group", "topic", "target", "code"},
	)
	prometheus.MustRegister(Http.PullFromPeer)

	Topic = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "topic_command_counter",
			Help:      "counter of topic commands",
		},
		[]string{"app", "topic", "action"},
	)
	prometheus.MustRegister(Topic)

	GC.Num = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "GC_num",
			Help:      "GC num",
		},
	)
	prometheus.MustRegister(GC.Num)

	GC.Duration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "GC_durations_milliseconds",
			Help:      "GC duration in milliseconds",
			Buckets:   []float64{0.5, 1, 10, 50, 100, 500, 1000, 2000, 10000},
		})
	prometheus.MustRegister(GC.Duration)

	Batch.Start = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "batch_start_total",
			Help:      "counter of started batch",
		},
	)
	prometheus.MustRegister(Batch.Start)

	Batch.Stop = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "batch_stop_total",
			Help:      "counter of stopped batch",
		},
	)
	prometheus.MustRegister(Batch.Stop)

	Replication.Total = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "replication_total",
			Help:      "replication total",
		},
		[]string{"src_dc"},
	)
	prometheus.MustRegister(Replication.Total)

	Replication.Errors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "replication_errors_total",
			Help:      "replication total errors",
		},
		[]string{"src_dc"},
	)
	prometheus.MustRegister(Replication.Errors)

	Replication.TopicTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "replication_topic_total",
			Help:      "topic replication total",
		},
		[]string{"src_dc", "topic"},
	)
	prometheus.MustRegister(Replication.TopicTotal)

	Replication.TopicErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "replication_topic_errors",
			Help:      "topic replication errors",
		},
		[]string{"src_dc", "topic"},
	)
	prometheus.MustRegister(Replication.TopicErrors)
}

func TrackHttpMetrics(apiName string) func(*gin.Context) {
	return func(c *gin.Context) {
		group := c.Param("group")
		topic := c.Param("topic")
		token := c.Query("token")
		if token == "" {
			token = c.PostForm("token")
		}

		before := time.Now()
		c.Next()
		after := time.Now()
		duration := after.Sub(before)

		Http.Latencies.WithLabelValues(
			token, group, topic, apiName).Observe(duration.Seconds() * 1000)
		Http.Code.WithLabelValues(
			token, group, topic, apiName, strconv.Itoa(c.Writer.Status())).Inc()
		Http.Bandwidth.WithLabelValues(token, "in").Add(float64(c.Request.ContentLength))
		Http.Bandwidth.WithLabelValues(token, "out").Add(float64(c.Writer.Size()))
	}
}
