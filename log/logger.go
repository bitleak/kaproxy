package log

import (
	"time"

	"github.com/meitu/kaproxy/log/hooks"
	"github.com/sirupsen/logrus"
)

const UpgradeKey = hooks.UpgradeKey

var Logger *logrus.Logger

func Init(logDir string) error {
	Logger = logrus.New()

	Logger.Hooks.Add(hooks.NewUpgradeHook(50, 3*time.Second))
	rollHook, err := hooks.NewRollHook(Logger, logDir, "kaproxy")
	if err != nil {
		return err
	}
	Logger.Hooks.Add(rollHook)
	Logger.Hooks.Add(hooks.NewSourceHook(logrus.WarnLevel))
	return nil
}
