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

type privateQ struct {
	sync.Mutex
	idgen         uint32
	nodeCount     int
	chunkCount    int
	maxChunkCount int
	head          *outQNode
	tail          *outQNode
}

type OutQueue struct {
	sync.Mutex
	chunkSize          int
	lastPop            time.Time
	waitPopForMillisec time.Duration
	mainQ              *privateQ
	spareQ             *privateQ
}

const (
	maxSpareCount = 1000
)

func NewOutQueue(chunkSize, maxCount int, waitPopForMillisec time.Duration) *OutQueue {
	chunkSize = lib.MinInt(500, lib.MaxInt(10, lib.MaxInt(0, chunkSize)))

	if maxCount <= 0 {
		maxCount = 10000
	}
	maxCount = lib.MinInt(10000, maxCount)
	waitPopForMillisec = time.Duration(lib.MinInt64(500, lib.MaxInt64(0, int64(waitPopForMillisec)))) * time.Millisecond

	return &OutQueue{
		chunkSize:          chunkSize,
		waitPopForMillisec: waitPopForMillisec,
		lastPop:            time.Now(),
		mainQ: &privateQ{
			maxChunkCount: maxCount,
		},
		spareQ: &privateQ{
			maxChunkCount: maxCount,
		},
	}
}

func (pq *privateQ) nextID() uint32 {
	pq.idgen++
	id := pq.idgen
	if id == math.MaxUint32 {
		pq.idgen = 0
	}

	return id
}

func (pq *privateQ) popFromQ() *outQNode {
	if pq.head != nil {
		n := pq.head

		pq.head = pq.head.next
		n.next = nil

		if pq.head != nil {
			pq.head.prev = nil
		} else {
			pq.tail = nil
		}

		pq.nodeCount--
		if pq.nodeCount < 0 {
			pq.nodeCount = 0
		}

		if pq.nodeCount == 0 {
			pq.head, pq.tail = nil, nil
		}
		return n
	}
	return nil
}

func (pq *privateQ) append(n *outQNode, chunkSize int) *outQNode {
	if n == nil {
		n = &outQNode{
			id:    pq.nextID(),
			prev:  pq.tail,
			chunk: make([]string, 0, chunkSize),
		}
	} else if len(n.chunk) == 0 {
		n.chunk = make([]string, 0, chunkSize)
	}

	if pq.tail == nil {
		pq.head, pq.tail = n, n
	} else {
		pq.tail.next, pq.tail = n, n
	}

	pq.nodeCount++

	return n
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

	mq := q.mainQ

	n := mq.tail
	if n == nil || len(n.chunk) >= q.chunkSize {
		n = mq.append(q.spareQ.popFromQ(), q.chunkSize)
	}

	n.chunk = append(n.chunk, data)
	mq.chunkCount++

	for mq.maxChunkCount > 0 &&
		mq.chunkCount > 1 &&
		mq.chunkCount > mq.maxChunkCount {
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
	mq := q.mainQ
	return mq.tail == nil || (mq.chunkCount < mq.maxChunkCount)
}

func (q *OutQueue) CanPop() bool {
	q.Lock()
	ready := q.popReady()
	q.Unlock()

	return ready
}

func (q *OutQueue) popReady() bool {
	return q.mainQ.head != nil &&
		(len(q.mainQ.head.chunk) >= q.chunkSize ||
			(q.waitPopForMillisec > 0 && time.Now().Sub(q.lastPop) >= q.waitPopForMillisec))

}

func (q *OutQueue) Pop(force bool) (chunk []string, ok bool) {
	q.Lock()
	defer q.Unlock()

	return q.popData(force)
}

func (q *OutQueue) pushToSpareQ(n *outQNode) {
	if n != nil {
		sq := q.spareQ
		if sq.nodeCount < maxSpareCount {
			if sq.tail == nil {
				sq.head, sq.tail = n, n
			} else {
				sq.tail.next, sq.tail = n, n
			}

			sq.nodeCount++
		}
	}
}

func (q *OutQueue) popData(force bool) (chunk []string, ok bool) {
	if !(force || q.popReady()) {
		return nil, false
	}

	mq := q.mainQ
	n := mq.popFromQ()

	q.lastPop = time.Now()

	if n != nil {
		chunk = n.chunk
		n.chunk = nil

		mq.chunkCount -= len(chunk)
		if mq.chunkCount < 0 {
			mq.chunkCount = 0
		}

		q.pushToSpareQ(n)

		return chunk, true
	}
	return nil, false
}

func (q *OutQueue) ChunkSize() int {
	return q.chunkSize
}

func (q *OutQueue) Count() int {
	q.Lock()
	count := q.mainQ.chunkCount
	q.Unlock()

	return count
}

func (q *OutQueue) NodeCount() int {
	q.Lock()
	ncount := q.mainQ.nodeCount
	q.Unlock()

	return ncount
}
