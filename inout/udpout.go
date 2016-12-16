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
	"bytes"
	"encoding/binary"
	"net"
	"reflect"

	"github.com/ocdogan/fluentgo/lib"
)

type udpOut struct {
	outHandler
	tcpUDPIO
	conn *net.UDPConn
}

func init() {
	RegisterOut("udp", newUDPOut)
	RegisterOut("udpout", newUDPOut)
}

func newUDPOut(manager InOutManager, params map[string]interface{}) OutSender {
	tuio := newTCPUDPIO(manager, params)
	if tuio == nil {
		return nil
	}

	oh := newOutHandler(manager, params)
	if oh == nil {
		return nil
	}

	tout := &udpOut{
		outHandler: *oh,
		tcpUDPIO:   *tuio,
	}

	tout.iotype = "UDPOUT"

	tout.runFunc = tout.funcWait
	tout.afterCloseFunc = tout.funcAfterClose

	tout.getDestinationFunc = tout.funcChannel
	tout.sendChunkFunc = tout.funcSendMessagesChunk

	return tout
}

func (tout *udpOut) funcChannel() string {
	return "null"
}

func (tout *udpOut) funcAfterClose() {
	conn := tout.conn
	if conn != nil {
		tout.conn = nil
		tout.tryToCloseConn(conn)
	}
}

func (tout *udpOut) Connect() {
	defer func() {
		if err := recover(); err != nil {
			if tout.logger != nil {
				tout.logger.Panic(err)
			}
		}
	}()

	conn := tout.conn

	hasConn := conn != nil
	if hasConn {
		v := reflect.ValueOf(conn)
		hasConn = !v.IsNil()
	}

	var connErr error
	if connErr != nil || !hasConn {
		if hasConn {
			tout.tryToCloseConn(conn)
		}

		conn = nil
		connErr = nil

		var addr *net.UDPAddr

		addr, connErr = net.ResolveUDPAddr("udp", tout.host)
		if connErr == nil {
			conn, connErr = net.DialUDP("udp", nil, addr)
		}

		tout.conn = conn
		if connErr != nil {
			tout.tryToCloseConn(conn)
		}
	}
}

func (tout *udpOut) funcWait() {
	defer func() {
		recover()
		l := tout.GetLogger()
		if l != nil {
			l.Println("Stoping 'UDPOUT'...")
		}
	}()

	l := tout.GetLogger()
	if l != nil {
		l.Println("Starting 'UDPOUT'...")
	}

	tout.Connect()

	<-tout.completed
}

func (tout *udpOut) funcSendMessagesChunk(messages []ByteArray, channel string) {
	if len(messages) > 0 {
		m := tout.GetManager()
		if m == nil {
			return
		}

		defer recover()

		var (
			err  error
			body []byte
		)

		stamp := make([]byte, 4)

		for _, msg := range messages {
			if !(err == nil && tout.Processing() && m.Processing()) {
				break
			}

			if len(msg) > 0 {
				err = func() error {
					var sendErr error
					defer func() {
						sendErr, _ = recover().(error)
					}()

					tout.Connect()

					conn := tout.conn
					if conn != nil {
						body = []byte(msg)
						if tout.compressed {
							body = lib.Compress(body, tout.compressType)
						}

						b := bytes.NewBuffer([]byte(lib.TCPUDPMsgStart))

						binary.BigEndian.PutUint32(stamp, uint32(len(body)))
						b.Write(stamp)

						b.Write(body)
						b.Write([]byte(lib.TCPUDPMsgEnd))

						conn.Write(b.Bytes())
					}
					return sendErr
				}()
			}
		}
	}
}
