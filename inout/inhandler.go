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

import "github.com/ocdogan/fluentgo/lib"

type inHandler struct {
	ioHandler
}

func newInHandler(manager InOutManager, params map[string]interface{}) *inHandler {
	ioh := newIOHandler(manager, params)
	if ioh == nil {
		return nil
	}

	return &inHandler{
		ioHandler: *ioh,
	}
}

func (ih *inHandler) getMaxMessageSize() int {
	msgSize := -1
	if ih.manager != nil {
		msgSize = ih.manager.GetMaxMessageSize()
	}
	return lib.MinInt(lib.InvalidMessageSize, lib.MaxInt(-1, msgSize))
}

func (ih *inHandler) queueMessage(data []byte, maxMsgSize int) {
	ln := len(data)
	if ln > 0 && (maxMsgSize < 1 || ln <= maxMsgSize) {
		defer recover()

		m := ih.GetManager()
		if m != nil {
			q := m.GetInQueue()

			if q != nil {
				if ih.compressed {
					decdata := lib.Decompress(data, ih.compressType)
					if decdata != nil {
						q.Push(decdata)
						return
					}
				}
				q.Push(data)
			}
		}
	}
}
