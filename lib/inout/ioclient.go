package inout

type ioClient interface {
	Run()
	Enabled() bool
	GetIOType() string
	Processing() bool
	Close()
}
