package inout

import (
	"encoding/binary"
	"net"
	"strings"
	"time"

	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/lib/config"
	"github.com/ocdogan/fluentgo/lib/log"
)

type tcpOut struct {
	outHandler
	host           string
	connTimeoutSec int
	compressed     bool
	conn           *net.TCPConn
	logger         log.Logger
}

func newTCPOut(manager InOutManager, config *config.InOutConfig) *tcpOut {
	if config == nil {
		return nil
	}

	params := config.GetParamsMap()

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

	oh := newOutHandler(manager, params)
	if oh == nil {
		return nil
	}

	var (
		f              float64
		connTimeoutSec int
		compressed     bool
	)

	if f, ok = params["connTimeoutSec"].(float64); ok {
		connTimeoutSec = lib.MinInt(30, lib.MaxInt(0, int(f)))
	}

	compressed, ok = params["compressed"].(bool)

	tout := &tcpOut{
		outHandler:     *oh,
		host:           host,
		compressed:     compressed,
		connTimeoutSec: connTimeoutSec,
		logger:         manager.GetLogger(),
	}

	tout.iotype = "TCPOUT"

	tout.runFunc = tout.funcWait
	tout.afterCloseFunc = tout.funcAfterClose

	tout.getDestinationFunc = tout.funcChannel
	tout.sendChunkFunc = tout.funcSendMessagesChunk

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

func (tout *tcpOut) tryToCloseConn(conn *net.TCPConn) error {
	var closeErr error
	if conn != nil {
		defer func() {
			err := recover()
			if closeErr == nil {
				closeErr, _ = err.(error)
			}
		}()
		closeErr = conn.Close()
	}
	return closeErr
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

	var connErr error
	if connErr != nil || !hasConn {
		if hasConn {
			tout.tryToCloseConn(conn)
		}

		conn = nil
		connErr = nil

		var c net.Conn
		if tout.connTimeoutSec <= 0 {
			c, connErr = net.Dial("tcp", tout.host)
		} else {
			c, connErr = net.DialTimeout("tcp", tout.host, time.Duration(tout.connTimeoutSec)*time.Second)
		}

		if connErr == nil {
			var ok bool
			if conn, ok = c.(*net.TCPConn); !ok {
				conn = nil
			}
		}

		tout.conn = conn
		if connErr != nil {
			tout.tryToCloseConn(conn)
		}
	}
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
							body = lib.Compress(body)
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
