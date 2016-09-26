package main

type InOutManager interface {
	Close()
	GetLogger() Logger
	GetMaxMessageSize() int
	GetInQueue() *inQueue
	GetOutQueue() *outQueue
	Process() (completed <-chan bool)
	Processing() bool
	HandleOrphans()
}
