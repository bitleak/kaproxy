package util

import (
	"log"
	"os"
	"runtime/debug"

	"github.com/sirupsen/logrus"
)

func WithRecover(logger *logrus.Logger, funcs ...func()) {
	if err := recover(); err != nil {
		if logger != nil {
			logger.WithFields(logrus.Fields{
				"err":    err,
				"statck": string(debug.Stack()),
			}).Error("Panic recovered")
		} else {
			defaultLogger := log.New(os.Stdout, "[Recovery] panic recovered:\n", log.LstdFlags)
			defaultLogger.Printf("%s\n%s\n", err, string(debug.Stack()))
		}
	}

	defer func() {
		if err := recover(); err != nil {
			if logger != nil {
				logger.WithFields(logrus.Fields{
					"err":    err,
					"statck": string(debug.Stack()),
				}).Error("Panic recovered")
			} else {
				defaultLogger := log.New(os.Stdout, "[Recovery] panic recovered:\n", log.LstdFlags)
				defaultLogger.Printf("%s\n%s\n", err, string(debug.Stack()))
			}
		}
	}()
	for _, f := range funcs {
		f()
	}
}
