package inout

import (
	"math"
	"sync"
)

type inQNode struct {
	id   uint32
	prev *inQNode
	next *inQNode
	data []byte
}

type InQueue struct {
	sync.Mutex
	idgen    uint32
	cnt      int
	maxCount int
	sz       uint64
	maxSize  uint64
	head     *inQNode
	tail     *inQNode
	ready    chan bool
}

func NewInQueue(maxCount int, maxSize uint64) *InQueue {
	return &InQueue{
		maxCount: maxCount,
		maxSize:  maxSize,
		ready:    make(chan bool),
	}
}

func (q *InQueue) Ready() <-chan bool {
	return q.ready
}

func (q *InQueue) nextID() uint32 {
	q.idgen++
	id := q.idgen
	if id == math.MaxUint32 {
		q.idgen = 0
	}

	return id
}

func (q *InQueue) Push(data []byte) {
	q.Lock()
	defer q.Unlock()

	q.put(data)
	q.ready <- true
}

func (q *InQueue) put(data []byte) {
	n := &inQNode{
		id:   q.nextID(),
		data: data,
		prev: q.tail,
	}

	if q.tail == nil {
		q.head, q.tail = n, n
	} else {
		q.tail.next, q.tail = n, n
	}
	q.cnt++
	if data != nil {
		q.sz += uint64(len(data))
	}

	for (q.maxSize > 0 && q.sz > q.maxSize) ||
		(q.maxCount > 0 && q.cnt > 1 && q.cnt > q.maxCount) {
		q.popData()
	}
}

func (q *InQueue) Pop() (data []byte, ok bool) {
	q.Lock()
	defer q.Unlock()

	return q.popData()
}

func (q *InQueue) popData() (data []byte, ok bool) {
	if q.head != nil {
		n := q.head

		q.head = q.head.next
		n.next = nil

		if q.head != nil {
			q.head.prev = nil
		} else {
			q.tail = nil
		}

		if q.cnt > 0 {
			q.cnt--
		}

		if q.cnt == 0 {
			q.head, q.tail = nil, nil
		}

		data = n.data
		n.data = nil

		if data != nil {
			q.sz -= uint64(len(data))
			if q.sz < 0 {
				q.sz = 0
			}
		}

		return data, true
	}
	return nil, false
}

func (q *InQueue) Count() int {
	q.Lock()
	count := q.cnt
	q.Unlock()

	return count
}
