package main

type InOutManager interface {
	Close()
	DoSleep() bool
	GetQueue() *DataQueue
	GetLogger() Logger
	GetMaxMessageSize() int
	Process() (completed <-chan bool)
	Processing() bool
}
