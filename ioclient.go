package main

type ioClient interface {
	Run()
	Enabled() bool
	Processing() bool
	Close()
}
