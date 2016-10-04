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
	"crypto/tls"
	"encoding/binary"
	"net"
	"reflect"
	"time"

	"github.com/ocdogan/fluentgo/lib"
)

type tcpOut struct {
	outHandler
	tcpUDPIO
	connTimeoutSec int
	conn           net.Conn
}

func newTCPOut(manager InOutManager, params map[string]interface{}) *tcpOut {
	tuio := newTCPUDPIO(manager, params)
	if tuio == nil {
		return nil
	}

	oh := newOutHandler(manager, params)
	if oh == nil {
		return nil
	}

	var (
		ok             bool
		f              float64
		connTimeoutSec int
	)

	if f, ok = params["connTimeoutSec"].(float64); ok {
		connTimeoutSec = lib.MinInt(30, lib.MaxInt(0, int(f)))
	}

	tout := &tcpOut{
		outHandler:     *oh,
		tcpUDPIO:       *tuio,
		connTimeoutSec: connTimeoutSec,
	}

	tout.iotype = "TCPOUT"

	tout.runFunc = tout.funcWait
	tout.afterCloseFunc = tout.funcAfterClose

	tout.getDestinationFunc = tout.funcChannel
	tout.sendChunkFunc = tout.funcSendMessagesChunk
	tout.loadTLSFunc = tout.loadClientCert

	return tout
}

func (tout *tcpOut) funcChannel() string {
	return "null"
}

func (tout *tcpOut) funcAfterClose() {
	conn := tout.conn
	if conn != nil {
		tout.conn = nil
		tout.tryToCloseConn(conn)
	}
}

func (tout *tcpOut) Connect() {
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

		timeout := tout.connTimeoutSec > 0

		if tout.secure && tout.tlsConfig != nil {
			if !timeout {
				conn, connErr = tls.Dial("tcp", tout.host, tout.tlsConfig)
			} else {
				d := net.Dialer{Timeout: time.Duration(tout.connTimeoutSec) * time.Second}
				conn, connErr = tls.DialWithDialer(&d, "tcp", tout.host, tout.tlsConfig)
			}
		} else if !timeout {
			conn, connErr = net.Dial("tcp", tout.host)
		} else {
			conn, connErr = net.DialTimeout("tcp", tout.host, time.Duration(tout.connTimeoutSec)*time.Second)
		}

		tout.conn = conn
		if connErr != nil {
			tout.tryToCloseConn(conn)
		}
	}
}

func (tout *tcpOut) loadClientCert() (secure bool, config *tls.Config, err error) {
	config, err = lib.LoadClientCert(tout.certFile, tout.keyFile, tout.caFile, tout.verifySsl)
	secure = (err == nil) && (config != nil)
	return
}

func (tout *tcpOut) funcWait() {
	defer func() {
		recover()
		l := tout.GetLogger()
		if l != nil {
			l.Println("Stoping 'TCPOUT'...")
		}
	}()

	l := tout.GetLogger()
	if l != nil {
		l.Println("Starting 'TCPOUT'...")
	}

	err := tout.loadCert()
	if err != nil {
		return
	}

	tout.Connect()

	<-tout.completed
}

func (tout *tcpOut) funcSendMessagesChunk(messages []string, channel string) {
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

			if msg != "" {
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

						conn.Write([]byte(lib.TCPUDPMsgStart))

						binary.BigEndian.PutUint32(stamp, uint32(len(body)))
						conn.Write(stamp)

						conn.Write(body)

						conn.Write([]byte(lib.TCPUDPMsgEnd))
					}
					return sendErr
				}()
			}
		}
	}
}
