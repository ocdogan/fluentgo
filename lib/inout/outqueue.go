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
	"math"
	"sync"
	"time"

	"github.com/ocdogan/fluentgo/lib"
)

type outQNode struct {
	id    uint32
	prev  *outQNode
	next  *outQNode
	chunk []string
}

type OutQueue struct {
	sync.Mutex
	idgen              uint32
	nodeCount          int
	count              int
	maxCount           int
	chunkSize          int
	lastPop            time.Time
	waitPopForMillisec time.Duration
	head               *outQNode
	tail               *outQNode
}

func NewOutQueue(chunkSize, maxCount int, waitPopForMillisec time.Duration) *OutQueue {
	chunkSize = lib.MinInt(500, lib.MaxInt(10, lib.MaxInt(0, chunkSize)))

	if maxCount <= 0 {
		maxCount = 10000
	}
	maxCount = lib.MinInt(10000, maxCount)
	waitPopForMillisec = time.Duration(lib.MinInt64(500, lib.MaxInt64(0, int64(waitPopForMillisec)))) * time.Millisecond

	return &OutQueue{
		chunkSize:          chunkSize,
		maxCount:           maxCount,
		waitPopForMillisec: waitPopForMillisec,
		lastPop:            time.Now(),
	}
}

func (q *OutQueue) nextID() uint32 {
	q.idgen++
	id := q.idgen
	if id == math.MaxUint32 {
		q.idgen = 0
	}

	return id
}

func (q *OutQueue) Push(data string) {
	if len(data) > 0 {
		q.Lock()
		defer q.Unlock()

		q.put(data)
	}
}

func (q *OutQueue) put(data string) {
	ln := len(data)
	if ln == 0 {
		return
	}

	n := q.tail
	if n == nil || len(n.chunk) >= q.chunkSize {
		n = &outQNode{
			id:    q.nextID(),
			prev:  q.tail,
			chunk: make([]string, 0, q.chunkSize),
		}

		if q.tail == nil {
			q.head, q.tail = n, n
		} else {
			q.tail.next, q.tail = n, n
		}

		q.nodeCount++
	}

	n.chunk = append(n.chunk, data)
	q.count++

	for q.maxCount > 0 &&
		q.count > 1 &&
		q.count > q.maxCount {
		q.popData(true)
	}
}

func (q *OutQueue) CanPush() bool {
	q.Lock()
	ready := q.pushReady()
	q.Unlock()

	return ready
}

func (q *OutQueue) pushReady() bool {
	return q.tail == nil || (q.count < q.maxCount)
}

func (q *OutQueue) CanPop() bool {
	q.Lock()
	ready := q.popReady()
	q.Unlock()

	return ready
}

func (q *OutQueue) popReady() bool {
	return q.head != nil &&
		(len(q.head.chunk) >= q.chunkSize ||
			(q.waitPopForMillisec > 0 && time.Now().Sub(q.lastPop) >= q.waitPopForMillisec))

}

func (q *OutQueue) Pop(force bool) (chunk []string, ok bool) {
	q.Lock()
	defer q.Unlock()

	return q.popData(force)
}

func (q *OutQueue) popData(force bool) (chunk []string, ok bool) {
	if !(force || q.popReady()) {
		return nil, false
	}

	q.lastPop = time.Now()
	if q.head != nil {
		n := q.head

		q.head = q.head.next
		n.next = nil

		if q.head != nil {
			q.head.prev = nil
		} else {
			q.tail = nil
		}

		q.nodeCount--
		if q.nodeCount < 0 {
			q.nodeCount = 0
		}

		if q.nodeCount == 0 {
			q.head, q.tail = nil, nil
		}

		chunk = n.chunk
		n.chunk = nil

		q.count -= len(chunk)
		if q.count < 0 {
			q.count = 0
		}

		return chunk, true
	}
	return nil, false
}

func (q *OutQueue) ChunkSize() int {
	return q.chunkSize
}

func (q *OutQueue) Count() int {
	q.Lock()
	count := q.count
	q.Unlock()

	return count
}

func (q *OutQueue) NodeCount() int {
	q.Lock()
	ncount := q.nodeCount
	q.Unlock()

	return ncount
}
