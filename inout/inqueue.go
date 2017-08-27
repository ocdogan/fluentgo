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
}

func NewInQueue(maxCount int, maxSize uint64) *InQueue {
	return &InQueue{
		maxCount: maxCount,
		maxSize:  maxSize,
	}
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
