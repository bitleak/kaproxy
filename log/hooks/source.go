package hooks

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/sirupsen/logrus"
)

type SourceHook struct {
	level logrus.Level
}

func NewSourceHook(level logrus.Level) *SourceHook {
	return &SourceHook{
		level: level,
	}
}

func (sh *SourceHook) Fire(entry *logrus.Entry) error {
	/*
	* The invocation chains in logrus is:
	* logrus.Info()->entry.Info() -> entry.log -> entry.Hooks.Fire -> MyHook.Fire
	* so we set skip start from 5, stop to 9(tricky number).
	 */
	for skip := 5; skip < 9; skip++ {
		if pc, file, line, ok := runtime.Caller(skip); ok {
			arr := strings.Split(file, "/")
			n := len(arr)
			if n > 1 && arr[n-2] == "logrus" {
				continue
			}
			funcName := runtime.FuncForPC(pc).Name()
			entry.Data["caller"] = fmt.Sprintf("%s:%d:%s", filepath.Base(file), line, funcName)
		}
		break
	}
	return nil
}

func (sh *SourceHook) Levels() []logrus.Level {
	levels := make([]logrus.Level, 0)
	for _, level := range logrus.AllLevels {
		if level <= sh.level {
			levels = append(levels, level)
		}
	}
	return levels
}
