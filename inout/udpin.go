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

	"github.com/ocdogan/fluentgo/config"
	"github.com/ocdogan/fluentgo/lib"
)

type udpIn struct {
	inHandler
	tcpUDPIO
	bufferSize int
	conn       *net.UDPConn
}

var (
	udpMsgEndChars   = []byte(lib.TCPUDPMsgEnd)
	udpMsgStartChars = []byte(lib.TCPUDPMsgStart)

	udpMsgEndLen   = len(udpMsgEndChars)
	udpMsgStartLen = len(udpMsgStartChars)
)

func init() {
	RegisterIn("udp", newUDPIn)
	RegisterIn("udpin", newUDPIn)
}

func newUDPIn(manager InOutManager, params map[string]interface{}) InProvider {
	tuio := newTCPUDPIO(manager, params)
	if tuio == nil {
		return nil
	}

	ih := newInHandler(manager, params)
	if ih == nil {
		return nil
	}

	bufferSize, ok := config.ParamAsIntWithLimit(params, "bufferSize", 1024, math.MaxInt16)
	if !ok {
		bufferSize = int(math.MaxInt16)
	}

	uin := &udpIn{
		inHandler:  *ih,
		tcpUDPIO:   *tuio,
		bufferSize: bufferSize,
	}

	uin.iotype = "UDPIN"

	uin.runFunc = uin.funcReceive
	uin.afterCloseFunc = uin.funcAfterClose
	uin.loadTLSFunc = uin.loadServerCert

	return uin
}

func (uin *udpIn) funcAfterClose() {
	if uin.conn != nil {
		defer recover()

		conn := uin.conn
		uin.conn = nil

		conn.Close()
	}
}

func (uin *udpIn) loadServerCert() (secure bool, config *tls.Config, err error) {
	config, err = lib.LoadServerCert(uin.certFile, uin.keyFile, uin.caFile, uin.verifySsl)
	secure = (err == nil) && (config != nil)
	return
}

func (uin *udpIn) listen(listenEnded chan bool) {
	lg := uin.logger
	if lg != nil {
		defer lg.Printf("'UDPIN' completed listening at '%s'.\n", uin.host)
		lg.Printf("'UDPIN' starting to listen at '%s'...\n", uin.host)
	}

	udpAddr, err := net.ResolveUDPAddr("udp", uin.host)
	if err != nil {
		if lg != nil {
			lg.Printf("'UDPIN' address error at '%s'.\n", uin.host)
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

	uin.conn = conn
	uin.accept()
}

func (uin *udpIn) accept() {
	if uin.conn == nil {
		return
	}

	buffer := make([]byte, uin.bufferSize)
	maxMessageSize := uin.getMaxMessageSize()

	lg := uin.logger
	for uin.Processing() {
		conn := uin.conn
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
			go uin.onNewMessage(buf)
		}
	}
}

func (uin *udpIn) onNewMessage(b []byte) {
	if len(b) >= udpMsgStartLen+udpMsgEndLen+4 {
		maxMessageSize := uin.getMaxMessageSize()

		expectedLen := int(binary.BigEndian.Uint32(b[udpMsgStartLen : udpMsgStartLen+4]))
		if expectedLen <= maxMessageSize {
			uin.queueMessage(b[udpMsgStartLen+4:], maxMessageSize)
		}
	}
}

func (uin *udpIn) funcReceive() {
	defer uin.InformStop()
	uin.InformStart()

	err := uin.loadCert()
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
		go uin.listen(listenEnded)

		select {
		case <-uin.completed:
			completed = true
			uin.Close()
			return
		case _, chanOpen = <-listenEnded:
			if completed {
				return
			}
		}
	}
}
