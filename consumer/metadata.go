package consumer

import (
	"github.com/bitleak/kaproxy/config"
)

type GroupMetadata struct {
	Owner     string                `json:"owner"`
	Topics    []string              `json:"topics"`
	Semantics string                `json:"semantics"`
	Consumer  config.ConsumerConfig `json:"consumer"`
	Stopped   bool                  `json:"stopped"`

	unackManager *unackManager
}
