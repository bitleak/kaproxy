package server

import (
	"github.com/gin-gonic/gin"
	"github.com/meitu/kaproxy/log"
	"github.com/sirupsen/logrus"
)

func getLogger(c *gin.Context) *logrus.Entry {
	if logger, ok := c.Get("logger"); !ok {
		return logrus.NewEntry(log.Logger)
	} else {
		return logger.(*logrus.Entry)
	}
}
