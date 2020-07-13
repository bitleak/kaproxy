package log

import (
	"time"

	"github.com/bitleak/kaproxy/log/hooks"
	"github.com/sirupsen/logrus"
)

const UpgradeKey = hooks.UpgradeKey

var (
	AccessLogger *logrus.Logger
	ErrorLogger  *logrus.Logger
)

func Init(logFormat, logDir string) error {
	AccessLogger = logrus.New()
	ErrorLogger = logrus.New()
	if logFormat == "json" {
		AccessLogger.SetFormatter(&logrus.JSONFormatter{})
		ErrorLogger.SetFormatter(&logrus.JSONFormatter{})
	}

	accessRollHook, err := hooks.NewRollHook(AccessLogger, logDir, "access")
	if err != nil {
		return err
	}
	AccessLogger.Hooks.Add(accessRollHook)
	ErrorLogger.Hooks.Add(hooks.NewUpgradeHook(50, 3*time.Second))
	errorRollHook, err := hooks.NewRollHook(ErrorLogger, logDir, "kaproxy")
	if err != nil {
		return err
	}
	ErrorLogger.Hooks.Add(errorRollHook)
	ErrorLogger.Hooks.Add(hooks.NewSourceHook(logrus.WarnLevel))

	return nil
}
