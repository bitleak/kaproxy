package consumer

import (
	"github.com/meitu/kaproxy/config"
)

type GroupMetadata struct {
	Owner        string                `json:"owner"`
	Topics       []string              `json:"topics"`
	Semantics    string                `json:"semantics"`
	Consumer     config.ConsumerConfig `json:"consumer"`
	unackManager *unackManager
}
