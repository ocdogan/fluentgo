package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type tcpIn struct {
	inHandler
	lck         sync.Mutex
	host        string
	compressed  bool
	logger      Logger
	listener    *net.TCPListener
	connections []*net.TCPConn
}

func newTCPIn(manager InOutManager, config *inOutConfig) *tcpIn {
	if config == nil {
		return nil
	}

	params := config.getParamsMap()

	var (
		ok   bool
		s    string
		host string
	)

	if s, ok = params["host"].(string); ok {
		host = strings.TrimSpace(s)
	}
	if host == "" {
		return nil
	}

	ih := newInHandler(manager, params)
	if ih == nil {
		return nil
	}

	var compressed bool
	compressed, ok = params["compressed"].(bool)

	tin := &tcpIn{
		inHandler:  *ih,
		host:       host,
		compressed: compressed,
		logger:     manager.GetLogger(),
	}

	tin.iotype = "TCPIN"

	tin.runFunc = tin.funcReceive
	tin.afterCloseFunc = tin.funcAfterClose

	return tin
}

func (tin *tcpIn) funcAfterClose() {
	l := tin.listener
	if l != nil {
		tin.listener = nil

		defer recover()
		l.Close()
	}
}

func (tin *tcpIn) accept() {
	listener := tin.listener
	if listener == nil {
		return
	}

	lg := tin.logger
	for tin.Processing() {
		// Listen for an incoming connection.
		listener.SetDeadline(time.Now().Add(10 * time.Second))

		conn, err := listener.AcceptTCP()
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
			func(tin *tcpIn, conn *net.TCPConn) {
				defer tin.lck.Unlock()

				tin.lck.Lock()
				tin.connections = append(tin.connections, conn)
			}(tin, conn)

			go tin.onNewConnection(conn)
		}
	}
}

func (tin *tcpIn) listen(listenEnded chan bool) {
	lg := tin.logger
	if lg != nil {
		defer lg.Printf("'TCPIN' completed listening at '%s'.\n", tin.host)
		lg.Printf("'TCPIN' starting to listen at '%s'...\n", tin.host)
	}

	l, err := net.Listen("tcp", tin.host)
	if err != nil {
		close(listenEnded)
		return
	}

	// Close the listener when the application closes.
	defer func(nl net.Listener, ch chan bool) {
		defer recover()
		nl.Close()
		close(ch)
	}(l, listenEnded)

	listener, ok := l.(*net.TCPListener)
	if !ok {
		if err == nil && lg != nil {
			lg.Printf("Error on 'TCPIN' listening '%s'.\n", tin.host)
		}

		return
	}

	tin.listener = listener
	tin.accept()
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

func (tin *tcpIn) remove(conn *net.TCPConn) {
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

func (tin *tcpIn) onNewConnection(conn *net.TCPConn) {
	defer func() {
		recover()
		tin.remove(conn)
	}()

	// Make a buffer to hold incoming data.
	byt := make([]byte, 512)
	buf := bytes.NewBuffer(nil)

	start := -1
	expectedLen := -1

	endChars := []byte(tcpUDPMsgEnd)
	startChars := []byte(tcpUDPMsgStart)

	compressed := tin.compressed
	maxMessageSize := minInt(InvalidMessageSize, maxInt(-1, tin.manager.GetMaxMessageSize()))

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
							go tin.queueMessage(data[4:], maxMessageSize, compressed)
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
