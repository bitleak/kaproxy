package producer

import "errors"

var (
	errHashWithoutKey        = errors.New("hash partition without key")
	errInvalidPartitionType  = errors.New("invalid partition type")
	errInvalidPartitionIndex = errors.New("invalid partition index")
)
