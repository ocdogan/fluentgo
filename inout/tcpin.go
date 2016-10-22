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
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/ocdogan/fluentgo/lib"
)

type tcpIn struct {
	inHandler
	tcpUDPIO
	lck         sync.Mutex
	connections []net.Conn
	listener    *net.Listener
}

func init() {
	RegisterIn("tcp", newTCPIn)
	RegisterIn("tcpin", newTCPIn)
}

func newTCPIn(manager InOutManager, params map[string]interface{}) InProvider {
	tuio := newTCPUDPIO(manager, params)
	if tuio == nil {
		return nil
	}

	ih := newInHandler(manager, params)
	if ih == nil {
		return nil
	}

	tin := &tcpIn{
		inHandler: *ih,
		tcpUDPIO:  *tuio,
	}

	tin.iotype = "TCPIN"

	tin.runFunc = tin.funcReceive
	tin.afterCloseFunc = tin.funcAfterClose
	tin.loadTLSFunc = tin.loadServerCert

	return tin
}

func (tin *tcpIn) funcAfterClose() {
	if tin.listener != nil {
		defer recover()

		listener := *tin.listener
		tin.listener = nil

		listener.Close()
	}
}

func (tin *tcpIn) loadServerCert() (secure bool, config *tls.Config, err error) {
	config, err = lib.LoadServerCert(tin.certFile, tin.keyFile, tin.caFile, tin.verifySsl)
	secure = (err == nil) && (config != nil)
	return
}

func (tin *tcpIn) listen(listenEnded chan bool) {
	lg := tin.logger
	if lg != nil {
		defer lg.Printf("'TCPIN' completed listening at '%s'.\n", tin.host)
		lg.Printf("'TCPIN' starting to listen at '%s'...\n", tin.host)
	}

	var (
		err      error
		listener net.Listener
	)

	if tin.secure && tin.tlsConfig != nil {
		listener, err = tls.Listen("tcp", tin.host, tin.tlsConfig)
	} else {
		listener, err = net.Listen("tcp", tin.host)
	}

	if err != nil {
		close(listenEnded)
		return
	}

	// Close the listener when the application closes.
	defer func(l net.Listener, ch chan bool) {
		defer recover()
		l.Close()
		close(ch)
	}(listener, listenEnded)

	tin.listener = &listener
	tin.accept()
}

func (tin *tcpIn) accept() {
	if tin.listener == nil {
		return
	}

	listener := *tin.listener
	tcpL, _ := listener.(*net.TCPListener)

	lg := tin.logger
	for tin.Processing() {
		// Listen for an incoming connection.
		if tcpL != nil {
			tcpL.SetDeadline(time.Now().Add(10 * time.Second))
		}

		conn, err := listener.Accept()
		if err != nil {
			errStr := err.Error()
			if _, ok := err.(*net.OpError); ok {
				if strings.Contains(errStr, "closed network") {
					return
				}
				if strings.Contains(errStr, "accept") && strings.Contains(errStr, "timeout") {
					continue
				}
			}

			if lg != nil {
				lg.Println("Error on 'TCPIN' accepting: ", errStr)
			}
			continue
		}

		// Handle connections in a new goroutine.
		if conn != nil {
			func(tin *tcpIn, conn net.Conn) {
				defer tin.lck.Unlock()

				tin.lck.Lock()
				tin.connections = append(tin.connections, conn)
			}(tin, conn)

			go tin.onNewConnection(conn)
		}
	}
}

func (tin *tcpIn) funcReceive() {
	defer func() {
		recover()

		l := tin.GetLogger()
		if l != nil {
			l.Println("Stoping 'TCPIN'...")
		}
	}()

	l := tin.GetLogger()
	if l != nil {
		l.Println("Starting 'TCPIN'...")
	}

	err := tin.loadCert()
	if err != nil {
		return
	}

	completed := false
	var (
		chanOpen    bool
		listenEnded chan bool
	)

	for !completed {
		if !chanOpen {
			listenEnded = make(chan bool)
		}
		go tin.listen(listenEnded)

		select {
		case <-tin.completed:
			completed = true
			tin.Close()
			return
		case _, chanOpen = <-listenEnded:
			if completed {
				return
			}
		}
	}
}

func (tin *tcpIn) remove(conn net.Conn) {
	if conn == nil {
		return
	}

	defer func(tin *tcpIn) {
		recover()
		tin.lck.Unlock()
	}(tin)

	tin.lck.Lock()

	conns := tin.connections
	if len(conns) == 0 {
		return
	}

	for index, c := range conns {
		if c == conn {
			tin.connections = append(conns[:index], conns[index+1:]...)
			break
		}
	}
}

func (tin *tcpIn) onNewConnection(conn net.Conn) {
	defer func() {
		recover()
		tin.remove(conn)
	}()

	// Make a buffer to hold incoming data.
	byt := make([]byte, 512)
	buf := bytes.NewBuffer(nil)

	start := -1
	expectedLen := -1

	endChars := []byte(lib.TCPUDPMsgEnd)
	startChars := []byte(lib.TCPUDPMsgStart)

	maxMessageSize := tin.getMaxMessageSize()

	for {
		reqLen, err := conn.Read(byt)
		if err != nil {
			l := tin.logger
			if err == io.EOF {
				if l != nil {
					l.Printf("Closing connection on 'TCPIN' %s\n", conn.LocalAddr())
				}
				return
			}
			if l != nil {
				l.Println("Error on 'TCPIN' reading: ", err.Error())
			}
			continue
		}

		if reqLen == 0 {
			l := tin.logger
			if l != nil {
				l.Printf("Closing connection on 'TCPIN' %s\n", conn.LocalAddr())
			}
			conn.Close()
			return
		}

		buf.Write(byt)

		if start == 0 {
			b := buf.Bytes()

			if expectedLen == -1 {
				if len(b) >= len(startChars)+4 {
					expectedLen = int(binary.BigEndian.Uint32(b[len(startChars) : len(startChars)+4]))
					if expectedLen > maxMessageSize {
						l := tin.logger
						if err == io.EOF {
							if l != nil {
								l.Printf("Unxpected message size %d. Closing connection on 'TCPIN' %s\n",
									expectedLen, conn.LocalAddr())
							}
							return
						}
					}
				}
				continue
			}

			end := bytes.Index(b, endChars)
			if end > -1 {
				buf.Reset()
				start = -1

				dataLen := expectedLen
				expectedLen = -1

				if end > len(startChars) {
					if end == dataLen+len(startChars) {
						bNext := b[end+len(endChars):]
						if len(bNext) > 0 {
							buf.Write(bNext)
						}

						data := b[len(startChars):end]
						if len(data) > 4 {
							go tin.queueMessage(data[4:], maxMessageSize)
						}
					}
				}
			}
		} else {
			b := buf.Bytes()

			start = bytes.Index(b, startChars)
			if start != 0 {
				end := bytes.Index(b, endChars)
				if end > -1 {
					buf.Reset()

					bNext := b[end+len(endChars):]
					if len(bNext) > 0 {
						buf.Write(bNext)
					}
				}
			}
		}
	}
}
