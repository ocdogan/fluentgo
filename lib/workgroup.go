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
)

type WorkGroup struct {
	workerCount int
	wg          sync.WaitGroup
}

func (w *WorkGroup) Add(delta int) {
	if delta != 0 {
		w.workerCount += delta
		w.wg.Add(delta)
	}
}

func (w *WorkGroup) Done() {
	if w.workerCount > 0 {
		w.workerCount--
		w.wg.Done()
	}
}

func (w *WorkGroup) Count() int {
	return w.workerCount
}

func (w *WorkGroup) Wait() {
	if w.workerCount > 0 {
		w.wg.Wait()
		w.workerCount = 0
	}
}

func (w *WorkGroup) Close() {
	if w.workerCount > 0 {
		w.wg.Add(-w.workerCount)
		w.workerCount = 0
	}
}
