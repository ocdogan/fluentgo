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

const (
	In    = "in"
	Out   = "out"
	InOut = "inout"

	Publish = "PUBLISH"
	LPush   = "LPUSH"
	RPush   = "RPUSH"

	Subscribe       = "SUBSCRIBE"
	PSubscribe      = "PSUBSCRIBE"
	RPop            = "RPOP"
	LPop            = "LPOP"
	BrPop           = "BRPOP"
	BlPop           = "BLPOP"
	PSubscribechars = "*?[]"

	RecStart uint32 = 12345
	RecStop  uint32 = 54321

	MinBufferSize  = 8 * 1024
	MaxBufferSize  = 10 * 1024 * 1024
	MinBufferCount = 10
	MaxBufferCount = 1000

	DefaultOutBulkCount = 50

	MinLogSize = 2 * 1024
	MaxLogSize = 10 * 1024 * 1024

	DayAsSec  = 60 * 60 * 24
	DayAsMSec = DayAsSec * 1000

	InvalidMessageSize = 1024 * 1024
	ISO8601Time        = "2006-01-02T15:04:05.999-07:00"

	TCPUDPMsgStart = "///*["
	TCPUDPMsgEnd   = "]*\\\\\\"

	Byte  = 1
	KByte = 1024 * Byte
	MByte = 1024 * KByte
	GByte = 1024 * MByte
)
