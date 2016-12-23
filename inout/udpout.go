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

	uout := &udpOut{
		outHandler: *oh,
		tcpUDPIO:   *tuio,
	}

	uout.iotype = "UDPOUT"

	uout.runFunc = uout.funcWait
	uout.afterCloseFunc = uout.funcAfterClose

	uout.getDestinationFunc = uout.funcChannel
	uout.sendChunkFunc = uout.funcSendMessagesChunk

	return uout
}

func (uout *udpOut) funcChannel() string {
	return "null"
}

func (uout *udpOut) funcAfterClose() {
	conn := uout.conn
	if conn != nil {
		uout.conn = nil
		uout.tryToCloseConn(conn)
	}
}

func (uout *udpOut) Connect() {
	defer func() {
		if err := recover(); err != nil {
			if uout.logger != nil {
				uout.logger.Panic(err)
			}
		}
	}()

	conn := uout.conn

	hasConn := conn != nil
	if hasConn {
		v := reflect.ValueOf(conn)
		hasConn = !v.IsNil()
	}

	var connErr error
	if connErr != nil || !hasConn {
		if hasConn {
			uout.tryToCloseConn(conn)
		}

		conn = nil
		connErr = nil

		var addr *net.UDPAddr

		addr, connErr = net.ResolveUDPAddr("udp", uout.host)
		if connErr == nil {
			conn, connErr = net.DialUDP("udp", nil, addr)
		}

		uout.conn = conn
		if connErr != nil {
			uout.tryToCloseConn(conn)
		}
	}
}

func (uout *udpOut) funcWait() {
	defer uout.InformStop()
	uout.InformStart()

	uout.Connect()

	<-uout.completed
}

func (uout *udpOut) funcSendMessagesChunk(messages []ByteArray, channel string) {
	if len(messages) > 0 {
		m := uout.GetManager()
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
			if !(err == nil && uout.Processing() && m.Processing()) {
				break
			}

			if len(msg) > 0 {
				err = func() error {
					var sendErr error
					defer func() {
						sendErr, _ = recover().(error)
					}()

					uout.Connect()

					conn := uout.conn
					if conn != nil {
						body = []byte(msg)
						if uout.compressed {
							body = lib.Compress(body, uout.compressType)
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
