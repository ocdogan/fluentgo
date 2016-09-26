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
