package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

type tcpIn struct {
	inHandler
	host       string
	compressed bool
	listener   *net.TCPListener
	logger     Logger
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

func (tin *tcpIn) onNewConnection(conn net.Conn) {
	defer recover()

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

func (tin *tcpIn) funcAfterClose() {
	l := tin.listener
	if l != nil {
		tin.listener = nil

		defer recover()
		l.Close()
	}
}

func (tin *tcpIn) accept() {
	l := tin.listener
	if l != nil {
		// Close the listener when the application closes.
		defer l.Close()
		for tin.Processing() {
			// Listen for an incoming connection.
			conn, err := l.Accept()
			if err != nil && tin.logger != nil {
				tin.logger.Println("Error on 'TCPIN' accepting: ", err.Error())
				continue
			}

			// Handle connections in a new goroutine.
			go tin.onNewConnection(conn)
		}
	}
}

func (tin *tcpIn) listen() error {
	listener := tin.listener
	if listener != nil {
		f, err := listener.File()
		if err == nil && f != nil {
			return nil
		}

		func(l *net.TCPListener) {
			defer recover()
			l.Close()
		}(listener)
	}

	l, err := net.Listen("tcp", tin.host)
	if err != nil {
		return err
	}

	var ok bool
	listener, ok = l.(*net.TCPListener)
	if !ok {
		err = l.Close()
		if err == nil {
			err = fmt.Errorf("Error on 'TCPIN' listening '%s'.", tin.host)
		}
		return err
	}

	tin.listener = listener

	go tin.accept()

	return nil
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
	for {
		select {
		case <-tin.completed:
			completed = true
			tin.Close()
			continue
		default:
			if !completed {
				err := tin.listen()
				time.Sleep(100 * time.Millisecond)

				if err != nil {
					continue
				}
			}
		}

		if completed {
			return
		}
	}
}
