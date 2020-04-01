package hooks

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type testLogCountsHook struct {
	warnCount  int64
	errorCount int64
}

func (h *testLogCountsHook) Levels() []logrus.Level {
	return []logrus.Level{logrus.WarnLevel, logrus.ErrorLevel}
}

func (h *testLogCountsHook) Fire(entry *logrus.Entry) error {
	switch entry.Level {
	case logrus.WarnLevel:
		atomic.AddInt64(&h.warnCount, 1)
	case logrus.ErrorLevel:
		atomic.AddInt64(&h.errorCount, 1)
	}
	return nil
}

var null = &noOpWriter{}

type noOpWriter struct {
}

func (*noOpWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func TestUpgradeHook_Fire(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(null)

	upgradeHook := NewUpgradeHook(10, 1000*time.Millisecond)
	logger.Hooks.Add(upgradeHook)
	testHook := new(testLogCountsHook)
	logger.Hooks.Add(testHook)

	logger.WithField(UpgradeKey, "test1").Warn("warn log")

	assert.EqualValues(t, 1, testHook.warnCount)
}

func TestUpgradeHook_Warn_OverThreshold(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(null)

	upgradeHook := NewUpgradeHook(10, 200*time.Millisecond)
	testHook := new(testLogCountsHook)

	logger.Hooks.Add(upgradeHook)
	logger.Hooks.Add(testHook)

	wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 10; i++ {
				// with field UpgradeKey
				logger.WithField(UpgradeKey, "test1").Warn("message")
				// without field UpgradeKey
				logger.Warn("message")
			}
			wg.Done()
		}()
	}
	wg.Wait()

	assert.EqualValues(t, 60, testHook.warnCount)
	assert.EqualValues(t, 40, testHook.errorCount)
}

func TestUpgradeHook_Warn_KeepOverThreshold(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(null)

	upgradeHook := NewUpgradeHook(9, 50*time.Millisecond)
	testHook := new(testLogCountsHook)

	logger.Hooks.Add(upgradeHook)
	logger.Hooks.Add(testHook)

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 10; i++ {
				logger.WithField(UpgradeKey, "test").Warn("message")
				time.Sleep(40 * time.Millisecond)
			}
			wg.Done()
		}()
	}

	wg.Wait()
	assert.EqualValues(t, 9, testHook.warnCount)
	assert.EqualValues(t, 91, testHook.errorCount)
}

func TestUpgradeHook_Warn_UnderThreshold(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(null)

	upgradeHook := NewUpgradeHook(10, 50*time.Millisecond)
	testHook := new(testLogCountsHook)
	logger.Hooks.Add(upgradeHook)
	logger.Hooks.Add(testHook)

	wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 10; j++ {
				logger.WithField(UpgradeKey, "test").Warn("message")
				time.Sleep(50 * time.Millisecond)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	assert.EqualValues(t, 50, testHook.warnCount)
	assert.EqualValues(t, 0, testHook.errorCount)
}

// 300	   4852546 ns/op
func BenchmarkUpgradeHook_Fire_WithOutHook(b *testing.B) {
	logger := logrus.New()
	logger.SetOutput(null)

	wg := sync.WaitGroup{}
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(index int) {
			for i := 0; i < 1000; i++ {
				logger.WithField(UpgradeKey, fmt.Sprintf("index%d", i%3)).Warn("message")
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

// 300	   5380094 ns/op
// Adding upgradeHook spend 10.8% more time than without this hook in log
func BenchmarkUpgradeHook_Fire_WithHook(b *testing.B) {
	logger := logrus.New()
	logger.SetOutput(null)

	upgradeHook := NewUpgradeHook(50, 3*time.Second)
	logger.Hooks.Add(upgradeHook)

	wg := sync.WaitGroup{}
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(index int) {
			for i := 0; i < 1000; i++ {
				logger.WithField(UpgradeKey, fmt.Sprintf("index%d", i%3)).Warn("message")
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}
