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
	"reflect"
	"sync/atomic"
)

type ChanActionStatus int

const (
	ChanNotOK ChanActionStatus = iota
	ChanOK
	ChanError
	ChanClosed
)

type Chan struct {
	val      *reflect.Value
	kind     reflect.Kind
	size     int
	closed   int32
	regCount int64
}

func MakeChanBuffered(typ reflect.Type, size int) *Chan {
	if typ.Kind() != reflect.Chan {
		typ = reflect.ChanOf(reflect.BothDir, typ)
	}
	if size < 0 {
		size = 0
	}
	v := reflect.MakeChan(typ, size)
	return &Chan{
		val:  &v,
		size: size,
		kind: typ.Kind(),
	}
}

func MakeChan(typ reflect.Type) *Chan {
	return MakeChanBuffered(typ, 0)
}

func MakeChanString() *Chan {
	return MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf("")))
}

func MakeChanByte() *Chan {
	return MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(byte(0))))
}

func MakeChanByteArray() *Chan {
	return MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf([]byte{})))
}

func MakeChanBool() *Chan {
	return MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(true)))
}

func MakeChanInt() *Chan {
	return MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(int(0))))
}

func MakeChanInt8() *Chan {
	return MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(int8(0))))
}

func MakeChanInt16() *Chan {
	return MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(int16(0))))
}

func MakeChanInt32() *Chan {
	return MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(int32(0))))
}

func MakeChanInt64() *Chan {
	return MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(int64(0))))
}

func MakeChanUint() *Chan {
	return MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(uint(0))))
}

func MakeChanUint8() *Chan {
	return MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(uint8(0))))
}

func MakeChanUint16() *Chan {
	return MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(uint16(0))))
}

func MakeChanUint32() *Chan {
	return MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(uint32(0))))
}

func MakeChanUint64() *Chan {
	return MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(uint64(0))))
}

func MakeChanFloat32() *Chan {
	return MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(float32(0))))
}

func MakeChanFloat64() *Chan {
	return MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(float64(0))))
}

func MakeChanBufferedString(size int) *Chan {
	return MakeChanBuffered(reflect.ChanOf(reflect.BothDir, reflect.TypeOf("")), size)
}

func MakeChanBufferedByte(size int) *Chan {
	return MakeChanBuffered(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(byte(1))), size)
}

func MakeChanBufferedByteArray(size int) *Chan {
	return MakeChanBuffered(reflect.ChanOf(reflect.BothDir, reflect.TypeOf([]byte{})), size)
}

func MakeChanBufferedBool(size int) *Chan {
	return MakeChanBuffered(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(true)), size)
}

func MakeChanBufferedInt(size int) *Chan {
	return MakeChanBuffered(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(int(0))), size)
}

func MakeChanBufferedInt8(size int) *Chan {
	return MakeChanBuffered(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(int8(0))), size)
}

func MakeChanBufferedInt16(size int) *Chan {
	return MakeChanBuffered(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(int16(0))), size)
}

func MakeChanBufferedInt32(size int) *Chan {
	return MakeChanBuffered(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(int32(0))), size)
}

func MakeChanBufferedInt64(size int) *Chan {
	return MakeChanBuffered(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(int64(0))), size)
}

func MakeChanBufferedUint(size int) *Chan {
	return MakeChanBuffered(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(uint(0))), size)
}

func MakeChanBufferedUint8(size int) *Chan {
	return MakeChanBuffered(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(uint8(0))), size)
}

func MakeChanBufferedUint16(size int) *Chan {
	return MakeChanBuffered(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(uint16(0))), size)
}

func MakeChanBufferedUint32(size int) *Chan {
	return MakeChanBuffered(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(uint32(0))), size)
}

func MakeChanBufferedUint64(size int) *Chan {
	return MakeChanBuffered(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(uint64(0))), size)
}

func MakeChanBufferedFloat32(size int) *Chan {
	return MakeChanBuffered(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(float32(0))), size)
}

func MakeChanBufferedFloat64(size int) *Chan {
	return MakeChanBuffered(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(float64(0))), size)
}

func (c *Chan) Size() int {
	if c != nil {
		return c.size
	}
	return -1
}

func (c *Chan) Kind() reflect.Kind {
	if c != nil {
		return c.kind
	}
	return reflect.Invalid
}

func (c *Chan) UnderlyingChan() *reflect.Value {
	if c != nil {
		return nil
	}
	return c.val
}

func (c *Chan) AssumeReceiving() bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 {
		return false
	}

	atomic.AddInt64(&c.regCount, 1)
	return true
}

func (c *Chan) AssumeReceived() {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 {
		return
	}

	if atomic.AddInt64(&c.regCount, -1) < 0 {
		atomic.StoreInt64(&c.regCount, 0)
	}
	return
}

func (c *Chan) Receive() (v reflect.Value, stat ChanActionStatus) {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 {
		return reflect.Value{}, ChanClosed
	}

	atomic.AddInt64(&c.regCount, 1)
	defer func() {
		e := recover()
		if e != nil {
			stat = ChanError
		}
		atomic.AddInt64(&c.regCount, -1)
	}()

	var ok bool
	v, ok = c.val.TryRecv()
	if !ok {
		stat = ChanNotOK
	} else {
		stat = ChanOK
	}
	return
}

func (c *Chan) ReceiveString() (val string, stat ChanActionStatus) {
	var v reflect.Value
	v, stat = c.Receive()

	if stat == ChanOK {
		val = v.String()
	}
	return
}

func (c *Chan) ReceiveByte() (val byte, stat ChanActionStatus) {
	var v reflect.Value
	v, stat = c.Receive()

	if stat == ChanOK {
		val = v.Interface().(byte)
	}
	return
}

func (c *Chan) ReceiveByteArray() (val []byte, stat ChanActionStatus) {
	var v reflect.Value
	v, stat = c.Receive()

	if stat == ChanOK {
		val = v.Bytes()
	}
	return
}

func (c *Chan) ReceiveBool() (val bool, stat ChanActionStatus) {
	var v reflect.Value
	v, stat = c.Receive()

	if stat == ChanOK {
		val = v.Bool()
	}
	return
}

func (c *Chan) ReceiveInt() (val int, stat ChanActionStatus) {
	var v reflect.Value
	v, stat = c.Receive()

	if stat == ChanOK {
		val = int(v.Int())
	}
	return
}

func (c *Chan) ReceiveInt8() (val int8, stat ChanActionStatus) {
	var v reflect.Value
	v, stat = c.Receive()

	if stat == ChanOK {
		val = int8(v.Int())
	}
	return
}

func (c *Chan) ReceiveInt16() (val int16, stat ChanActionStatus) {
	var v reflect.Value
	v, stat = c.Receive()

	if stat == ChanOK {
		val = int16(v.Int())
	}
	return
}

func (c *Chan) ReceiveInt32() (val int32, stat ChanActionStatus) {
	var v reflect.Value
	v, stat = c.Receive()

	if stat == ChanOK {
		val = int32(v.Int())
	}
	return
}

func (c *Chan) ReceiveInt64() (val int64, stat ChanActionStatus) {
	var v reflect.Value
	v, stat = c.Receive()

	if stat == ChanOK {
		val = int64(v.Int())
	}
	return
}

func (c *Chan) ReceiveUint() (val uint, stat ChanActionStatus) {
	var v reflect.Value
	v, stat = c.Receive()

	if stat == ChanOK {
		val = uint(v.Uint())
	}
	return
}

func (c *Chan) ReceiveUint8() (val uint8, stat ChanActionStatus) {
	var v reflect.Value
	v, stat = c.Receive()

	if stat == ChanOK {
		val = uint8(v.Uint())
	}
	return
}

func (c *Chan) ReceiveUint16() (val uint16, stat ChanActionStatus) {
	var v reflect.Value
	v, stat = c.Receive()

	if stat == ChanOK {
		val = uint16(v.Uint())
	}
	return
}

func (c *Chan) ReceiveUint32() (val uint32, stat ChanActionStatus) {
	var v reflect.Value
	v, stat = c.Receive()

	if stat == ChanOK {
		val = uint32(v.Uint())
	}
	return
}

func (c *Chan) ReceiveUint64() (val uint64, stat ChanActionStatus) {
	var v reflect.Value
	v, stat = c.Receive()

	if stat == ChanOK {
		val = uint64(v.Uint())
	}
	return
}

func (c *Chan) ReceiveFloat32() (val float32, stat ChanActionStatus) {
	var v reflect.Value
	v, stat = c.Receive()

	if stat == ChanOK {
		val = float32(v.Float())
	}
	return
}

func (c *Chan) ReceiveFloat64() (val float64, stat ChanActionStatus) {
	var v reflect.Value
	v, stat = c.Receive()

	if stat == ChanOK {
		val = v.Float()
	}
	return
}

func (c *Chan) Send(v reflect.Value) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(v)
}

func (c *Chan) SendString(val string) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(reflect.ValueOf(val))
}

func (c *Chan) SendByte(val byte) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(reflect.ValueOf(val))
}

func (c *Chan) SendByteArray(val []byte) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(reflect.ValueOf(val))
}

func (c *Chan) SendBool(val bool) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(reflect.ValueOf(val))
}

func (c *Chan) SendInt(val int) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(reflect.ValueOf(val))
}

func (c *Chan) SendInt8(val int8) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(reflect.ValueOf(val))
}

func (c *Chan) SendInt16(val int16) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(reflect.ValueOf(val))
}

func (c *Chan) SendInt32(val int32) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(reflect.ValueOf(val))
}

func (c *Chan) SendInt64(val int64) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(reflect.ValueOf(val))
}

func (c *Chan) SendUint(val uint) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(reflect.ValueOf(val))
}

func (c *Chan) SendUint8(val uint8) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(reflect.ValueOf(val))
}

func (c *Chan) SendUint16(val uint16) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(reflect.ValueOf(val))
}

func (c *Chan) SendUint32(val uint32) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(reflect.ValueOf(val))
}

func (c *Chan) SendUint64(val uint64) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(reflect.ValueOf(val))
}

func (c *Chan) SendFloat32(val float32) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(reflect.ValueOf(val))
}

func (c *Chan) SendFloat64(val float64) bool {
	if c == nil || atomic.LoadInt32(&c.closed) != 0 ||
		atomic.LoadInt64(&c.regCount) == 0 {
		return false
	}
	return c.val.TrySend(reflect.ValueOf(val))
}

func (c *Chan) Closed() bool {
	return atomic.LoadInt32(&c.closed) != 0
}

func (c *Chan) Close() bool {
	if c != nil && atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		c.val.Close()
		return true
	}
	return false
}
