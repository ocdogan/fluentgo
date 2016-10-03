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
	"math"
	"net"
	"strings"
	"time"

	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/lib/config"
)

type udpIn struct {
	inHandler
	tcpUDPIO
	bufferSize int
	conn       *net.UDPConn
}

var (
	udpMsgEndLen   int
	udpMsgStartLen int

	udpMsgEndChars   []byte
	udpMsgStartChars []byte
)

func init() {
	udpMsgEndChars = []byte(lib.TCPUDPMsgEnd)
	udpMsgStartChars = []byte(lib.TCPUDPMsgStart)

	udpMsgEndLen = len(udpMsgEndChars)
	udpMsgStartLen = len(udpMsgStartChars)
}

func newUDPIn(manager InOutManager, config *config.InOutConfig) *udpIn {
	if config == nil {
		return nil
	}

	params := config.GetParamsMap()

	tuio := newTCPUDPIO(manager, params)
	if tuio == nil {
		return nil
	}

	ih := newInHandler(manager, params)
	if ih == nil {
		return nil
	}

	bufferSize := int(math.MaxInt16)
	if f, ok := params["bufferSize"].(float64); ok {
		bufferSize = lib.MinInt(int(math.MaxInt16), lib.MaxInt(1024, int(f)))
	}

	tin := &udpIn{
		inHandler:  *ih,
		tcpUDPIO:   *tuio,
		bufferSize: bufferSize,
	}

	tin.iotype = "UDPIN"

	tin.runFunc = tin.funcReceive
	tin.afterCloseFunc = tin.funcAfterClose
	tin.loadTLSFunc = tin.loadServerCert

	return tin
}

func (tin *udpIn) funcAfterClose() {
	if tin.conn != nil {
		defer recover()

		conn := tin.conn
		tin.conn = nil

		conn.Close()
	}
}

func (tin *udpIn) loadServerCert() (secure bool, config *tls.Config, err error) {
	config, err = lib.LoadServerCert(tin.certFile, tin.keyFile)
	secure = (err == nil) && (config != nil)
	return
}

func (tin *udpIn) listen(listenEnded chan bool) {
	lg := tin.logger
	if lg != nil {
		defer lg.Printf("'UDPIN' completed listening at '%s'.\n", tin.host)
		lg.Printf("'UDPIN' starting to listen at '%s'...\n", tin.host)
	}

	udpAddr, err := net.ResolveUDPAddr("udp", tin.host)
	if err != nil {
		if lg != nil {
			lg.Printf("'UDPIN' address error at '%s'.\n", tin.host)
		}
		return
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		close(listenEnded)
		return
	}

	// Close the listener when the application closes.
	defer func(c *net.UDPConn, ch chan bool) {
		defer recover()
		c.Close()
		close(ch)
	}(conn, listenEnded)

	tin.conn = conn
	tin.accept()
}

func (tin *udpIn) accept() {
	if tin.conn == nil {
		return
	}

	buffer := make([]byte, tin.bufferSize)
	maxMessageSize := tin.getMaxMessageSize()

	lg := tin.logger
	for tin.Processing() {
		conn := tin.conn
		// Listen for an incoming connection.
		if conn == nil {
			time.Sleep(time.Millisecond)
			continue
		}

		reqLen, _, err := conn.ReadFromUDP(buffer)
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
				lg.Println("Error on 'UDPIN' accepting: ", errStr)
			}
			continue
		}

		if reqLen > 0 && reqLen <= maxMessageSize {
			buf := make([]byte, reqLen)
			go tin.onNewMessage(buf)
		}
	}
}

func (tin *udpIn) onNewMessage(b []byte) {
	if len(b) >= udpMsgStartLen+udpMsgEndLen+4 {
		maxMessageSize := tin.getMaxMessageSize()

		expectedLen := int(binary.BigEndian.Uint32(b[udpMsgStartLen : udpMsgStartLen+4]))
		if expectedLen <= maxMessageSize {
			tin.queueMessage(b[udpMsgStartLen+4:], maxMessageSize)
		}
	}
}

func (tin *udpIn) funcReceive() {
	defer func() {
		recover()

		l := tin.GetLogger()
		if l != nil {
			l.Println("Stoping 'UDPIN'...")
		}
	}()

	l := tin.GetLogger()
	if l != nil {
		l.Println("Starting 'UDPIN'...")
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
