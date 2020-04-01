package hooks

import (
	"github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
	"time"
)

const UpgradeKey = "upgrade_category"

type UpgradeHook struct {
	counters  sync.Map
	threshold int64
	interval  time.Duration
	ticker    *time.Ticker
}

func NewUpgradeHook(threshold int64, interval time.Duration) *UpgradeHook {
	if interval < 0 {
		interval = 3 * time.Second
	}
	if threshold < 0 {
		threshold = 50
	}

	d := &UpgradeHook{
		counters:  sync.Map{},
		ticker:    time.NewTicker(interval),
		threshold: threshold,
	}
	d.startResetLoop()

	return d
}

// reset counters every interval
func (h *UpgradeHook) startResetLoop() {
	go func() {
		lastResetCounts := make(map[string]int64)
		for {
			_, ok := <-h.ticker.C
			if !ok {
				// if ticker is stopped, stop janitor
				break
			}

			h.counters.Range(func(key, value interface{}) bool {
				counter, _ := value.(*int64)
				if *counter-lastResetCounts[key.(string)] <= h.threshold {
					atomic.StoreInt64(counter, 0)
				}
				lastResetCounts[key.(string)] = *counter
				return true
			})
		}
	}()
}

func (*UpgradeHook) Levels() []logrus.Level {
	return []logrus.Level{logrus.WarnLevel}
}

func (h *UpgradeHook) Fire(entry *logrus.Entry) error {
	key, ok := entry.Data[UpgradeKey]
	if !ok {
		return nil
	}

	var initValue int64 = 0
	rawCount, _ := h.counters.LoadOrStore(key, &initValue)
	counter, _ := rawCount.(*int64)
	if atomic.AddInt64(counter, 1) > h.threshold {
		entry.Level = logrus.ErrorLevel
	}
	return nil
}

// In general, it's not required to close the UpgradeHook.
func (h *UpgradeHook) Close() {
	h.ticker.Stop()
}
