package server

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// AccessLogMiddleware generate accesslog and output
func AccessLogMiddleware(accessLogEnable bool, logger *logrus.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Start timer
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		// Process request
		c.Next()

		// Stop timer
		end := time.Now()
		latency := end.Sub(start)

		clientIP := c.ClientIP()
		method := c.Request.Method
		statusCode := c.Writer.Status()

		fields := logrus.Fields{
			"path":    path,
			"query":   query,
			"latency": latency,
			"ip":      clientIP,
			"method":  method,
			"code":    statusCode,
			"req_id":  c.Request.Header.Get("X-Request-Id"),
		}

		if !accessLogEnable {
			return
		}

		if statusCode >= 500 {
			logger.WithFields(fields).Error()
		} else if statusCode >= 400 && statusCode != 404 {
			logger.WithFields(fields).Warn()
		} else {
			logger.WithFields(fields).Info()
		}
	}
}
