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

package lib

import (
	"sync"
	"sync/atomic"
)

type WorkGroup struct {
	workerCount int64
	wg          sync.WaitGroup
}

func (w *WorkGroup) Add(delta int) {
	if delta != 0 {
		atomic.AddInt64(&w.workerCount, int64(delta))
		w.wg.Add(delta)
	}
}

func (w *WorkGroup) Done() {
	if atomic.AddInt64(&w.workerCount, -1) < 0 {
		atomic.StoreInt64(&w.workerCount, 0)
	} else {
		w.wg.Done()
	}
}

func (w *WorkGroup) Count() int {
	return int(w.workerCount)
}

func (w *WorkGroup) Wait() {
	if atomic.LoadInt64(&w.workerCount) > 0 {
		w.wg.Wait()
		atomic.StoreInt64(&w.workerCount, 0)
	}
}

func (w *WorkGroup) Close() {
	old := atomic.SwapInt64(&w.workerCount, 0)
	if old > 0 {
		w.wg.Add(int(-old))
	}
}
