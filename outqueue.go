package main

import (
	"math"
	"sync"
	"time"
)

type outQNode struct {
	id    uint32
	prev  *outQNode
	next  *outQNode
	chunk []string
}

type outQueue struct {
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

func newOutQueue(chunkSize, maxCount int, waitPopForMillisec time.Duration) *outQueue {
	chunkSize = minInt(500, maxInt(10, maxInt(0, chunkSize)))

	if maxCount <= 0 {
		maxCount = 10000
	}
	maxCount = minInt(10000, maxCount)
	waitPopForMillisec = time.Duration(minInt64(500, maxInt64(0, int64(waitPopForMillisec)))) * time.Millisecond

	return &outQueue{
		chunkSize:          chunkSize,
		maxCount:           maxCount,
		waitPopForMillisec: waitPopForMillisec,
		lastPop:            time.Now(),
	}
}

func (q *outQueue) nextID() uint32 {
	q.idgen++
	id := q.idgen
	if id == math.MaxUint32 {
		q.idgen = 0
	}

	return id
}

func (q *outQueue) Push(data string) {
	if len(data) > 0 {
		q.Lock()
		defer q.Unlock()

		q.put(data)
	}
}

func (q *outQueue) put(data string) {
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

func (q *outQueue) CanPush() bool {
	q.Lock()
	ready := q.pushReady()
	q.Unlock()

	return ready
}

func (q *outQueue) pushReady() bool {
	return q.tail == nil || (q.count < q.maxCount)
}

func (q *outQueue) CanPop() bool {
	q.Lock()
	ready := q.popReady()
	q.Unlock()

	return ready
}

func (q *outQueue) popReady() bool {
	return q.head != nil &&
		(len(q.head.chunk) >= q.chunkSize ||
			(q.waitPopForMillisec > 0 && time.Now().Sub(q.lastPop) >= q.waitPopForMillisec))

}

func (q *outQueue) Pop(force bool) (chunk []string, ok bool) {
	q.Lock()
	defer q.Unlock()

	return q.popData(force)
}

func (q *outQueue) popData(force bool) (chunk []string, ok bool) {
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

func (q *outQueue) ChunkSize() int {
	return q.chunkSize
}

func (q *outQueue) Count() int {
	q.Lock()
	count := q.count
	q.Unlock()

	return count
}

func (q *outQueue) NodeCount() int {
	q.Lock()
	ncount := q.nodeCount
	q.Unlock()

	return ncount
}
