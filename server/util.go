package server

import (
	"github.com/bitleak/kaproxy/log"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

func getLogger(c *gin.Context) *logrus.Entry {
	if logger, ok := c.Get("logger"); !ok {
		return logrus.NewEntry(log.ErrorLogger)
	} else {
		return logger.(*logrus.Entry)
	}
}
