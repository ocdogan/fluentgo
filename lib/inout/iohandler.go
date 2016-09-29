//	The MIT License (MIT)
//
//	Copyright (c) 2016, Cagatay Dogan
//
//	Permission is hereby granted, free of charge, to any person obtaining a copy
//	of this software and associated documentation files (the "Software"), to deal
//	in the Software without restriction, including without limitation the rights
//	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//	copies of the Software, and to permit persons to whom the Software is
//	furnished to do so, subject to the following conditions:
//
//		The above copyright notice and this permission notice shall be included in
//		all copies or substantial portions of the Software.
//
//		THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//		IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//		FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//		AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//		LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//		OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//		THE SOFTWARE.

package inout

import (
	"sync/atomic"

	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/lib/log"
)

type ioHandler struct {
	id              lib.UUID
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
	id, err := lib.NewUUID()
	if err != nil {
		return nil
	}

	enabled := true
	if params != nil {
		var ok bool
		enabled, ok = params["enabled"].(bool)
		if !ok {
			enabled = true
		}
	}

	return &ioHandler{
		id:        *id,
		enabled:   enabled,
		manager:   manager,
		completed: make(chan bool),
	}
}

func (ioh *ioHandler) ID() lib.UUID {
	return ioh.id
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
