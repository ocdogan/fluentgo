package inout

import (
	"sync/atomic"

	"github.com/ocdogan/fluentgo/lib/log"
)

type ioHandler struct {
	enabled         bool
	iotype          string
	processing      int32
	runFunc         func()
	beforeCloseFunc func()
	afterCloseFunc  func()
	manager         InOutManager
	completed       chan bool
}

func newIOHandler(manager InOutManager, params map[string]interface{}) *ioHandler {
	enabled := true
	if params != nil {
		var ok bool
		enabled, ok = params["enabled"].(bool)
		if !ok {
			enabled = true
		}
	}

	return &ioHandler{
		enabled:   enabled,
		manager:   manager,
		completed: make(chan bool),
	}
}

func (ioh *ioHandler) GetIOType() string {
	return ioh.iotype
}

func (ioh *ioHandler) GetLogger() log.Logger {
	m := ioh.manager
	if m != nil {
		return m.GetLogger()
	}
	return nil
}

func (ioh *ioHandler) GetManager() InOutManager {
	return ioh.manager
}

func (ioh *ioHandler) Enabled() bool {
	return ioh.enabled
}

func (ioh *ioHandler) Processing() bool {
	return atomic.LoadInt32(&ioh.processing) != 0
}

func (ioh *ioHandler) SetProcessing(ok bool) {
	if ok {
		atomic.StoreInt32(&ioh.processing, 1)
	} else {
		atomic.StoreInt32(&ioh.processing, 0)
	}
}

func (ioh *ioHandler) Close() {
	defer func() {
		recover()

		ioh.SetProcessing(false)
		if ioh.afterCloseFunc != nil {
			ioh.afterCloseFunc()
		}
	}()

	if ioh.beforeCloseFunc != nil {
		ioh.beforeCloseFunc()
	}

	if ioh.Processing() {
		c := ioh.completed
		if c != nil {
			defer func() {
				ioh.completed = nil
			}()
			close(c)
		}
	}
}

func (ioh *ioHandler) Run() {
	if atomic.CompareAndSwapInt32(&ioh.processing, 0, 1) {
		defer func() {
			defer recover()

			completed := atomic.CompareAndSwapInt32(&ioh.processing, 1, 0)
			func() {
				defer recover()
				ioh.Close()
			}()

			if !completed && ioh.completed != nil {
				func() {
					defer recover()
					ioh.completed <- true
				}()
			}
		}()

		if ioh.runFunc != nil {
			ioh.runFunc()
		}
	}
}
