package consumer

import "errors"

var (
	ErrGroupStopped         = errors.New("consumer group was stopped")
	ErrGroupNotFound        = errors.New("consumer group not found")
	ErrTopicNotFound        = errors.New("topic not found in this consumer group")
	ErrNoMessage            = errors.New("no message in brokers")
	ErrUnackManagerStopped  = errors.New("unack manager was stopped")
	ErrUnackMessageNotFound = errors.New("unack message not found")
	ErrGroupNotAllowACK     = errors.New("consumer group is not allowed to be acknowledged")
	ErrNoPartition          = errors.New("consumer group doesn't claim any partition")
	ErrNoPeer               = errors.New("no peer can pull message")
)
