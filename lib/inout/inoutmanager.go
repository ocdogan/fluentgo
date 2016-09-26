package inout

import "github.com/ocdogan/fluentgo/lib/log"

type InOutManager interface {
	Close()
	GetLogger() log.Logger
	GetMaxMessageSize() int
	GetInQueue() *InQueue
	GetOutQueue() *OutQueue
	Process() (completed <-chan bool)
	Processing() bool
	HandleOrphans()
}
