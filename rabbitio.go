package main

import (
	"math"
	"net"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

type rabbitIO struct {
	port         int
	host         string
	command      string
	username     string
	password     string
	vhost        string
	queue        string
	key          string
	tag          string
	exchange     string
	exchangeType string
	compressed   bool
	connected    bool
	timeout      time.Duration
	connFunc     func(*amqp.Connection, *amqp.Channel) error
	conn         *amqp.Connection
	channel      *amqp.Channel
	logger       Logger
}

func newRabbitIO(logger Logger, params map[string]interface{}) *rabbitIO {
	if params == nil {
		return nil
	}

	var (
		ok           bool
		port         int
		f            float64
		s            string
		host         string
		vhost        string
		username     string
		password     string
		queue        string
		key          string
		tag          string
		exchange     string
		exchangeType string
		timeout      time.Duration
	)

	s, ok = params["queue"].(string)
	if ok {
		queue = strings.TrimSpace(s)
	}
	if queue == "" {
		return nil
	}

	s, ok = params["key"].(string)
	if ok {
		key = strings.TrimSpace(s)
	}
	if key == "" {
		return nil
	}

	s, ok = params["tag"].(string)
	if ok {
		tag = strings.TrimSpace(s)
	}

	s, ok = params["exchange"].(string)
	if ok {
		exchange = strings.TrimSpace(s)
	}
	if exchange == "" {
		return nil
	}

	s, ok = params["exchangeType"].(string)
	if ok {
		exchangeType = strings.TrimSpace(s)
	}
	if exchangeType == "" {
		return nil
	}

	s, ok = params["host"].(string)
	if ok {
		host = strings.TrimSpace(s)
	}
	if host == "" {
		return nil
	}

	f, ok = params["port"].(float64)
	if !ok {
		port = 5672
	} else {
		port = maxInt(math.MaxInt16, minInt(0, int(f)))
	}

	f, ok = params["timeout"].(float64)
	if ok {
		timeout = time.Duration(maxInt(60, minInt(0, int(f))))
	}

	s, ok = params["vhost"].(string)
	if ok {
		vhost = strings.TrimSpace(s)
	}

	s, ok = params["username"].(string)
	if ok {
		username = strings.TrimSpace(s)
	}

	s, ok = params["password"].(string)
	if ok {
		password = strings.TrimSpace(s)
	}

	var compressed bool
	compressed, ok = params["compressed"].(bool)

	rio := &rabbitIO{
		host:         host,
		port:         port,
		vhost:        vhost,
		username:     username,
		password:     password,
		compressed:   compressed,
		timeout:      timeout,
		queue:        queue,
		key:          key,
		tag:          tag,
		exchange:     exchange,
		exchangeType: exchangeType,
		logger:       logger,
	}

	return rio
}

func (rio *rabbitIO) funcAfterClose() {
	defer recover()

	conn := rio.conn
	if conn != nil {
		channel := rio.channel

		rio.channel = nil
		rio.conn = nil
		rio.connected = false

		if channel != nil {
			channel.Close()
		}
		conn.Close()
	}
}

func (rio *rabbitIO) tryToCloseConn(conn *amqp.Connection) {
	if conn != nil {
		defer recover()
		conn.Close()
	}
}

func (rio *rabbitIO) waitClose(conn *amqp.Connection) {
	go func() {
		<-conn.NotifyClose(make(chan *amqp.Error))

		rio.connected = false
		rio.conn = nil
		rio.channel = nil
	}()
}

func (rio *rabbitIO) Connected() bool {
	return rio.conn != nil && rio.connected
}

func (rio *rabbitIO) Connect() {
	defer func() {
		if err := recover(); err != nil {
			if rio.logger != nil {
				rio.logger.Panic(err)
			}
		}
	}()

	conn := rio.conn

	if !rio.Connected() {
		url := amqp.URI{
			Scheme:   "amqp",
			Host:     rio.host,
			Port:     rio.port,
			Username: rio.username,
			Password: rio.password,
			Vhost:    rio.vhost,
		}.String()

		if rio.logger != nil {
			rio.logger.Printf("Connectiong to 'RABBIT' on '%s'...\n", url)
		}

		var (
			connErr error
			channel *amqp.Channel
		)

		defer func() {
			if connErr == nil {
				rio.conn = conn
				rio.channel = channel
				rio.connected = true
			} else {
				rio.conn = nil
				rio.channel = nil
				rio.connected = false
			}
		}()

		if rio.timeout > 0 {
			conn, connErr = amqp.DialConfig(url, amqp.Config{
				Dial: func(network, addr string) (net.Conn, error) {
					return net.DialTimeout(network, addr, rio.timeout*time.Second)
				},
			})
		} else {
			conn, connErr = amqp.Dial(url)
		}

		if connErr != nil {
			if rio.logger != nil {
				rio.logger.Printf("Error connectiong to 'RABBIT' on '%s'. Error: %s.\n", url, connErr)
			}
			return
		}

		if conn != nil {
			channel, connErr = conn.Channel()
			if connErr != nil {
				if rio.logger != nil {
					rio.logger.Printf("Channel error for 'RABBIT' on '%s'. Error: %s.\n", url, connErr)
				}

				c := conn
				conn = nil
				rio.tryToCloseConn(c)
			}
		}

		if conn != nil {
			rio.waitClose(conn)

			if rio.connFunc != nil {
				connErr = func() error {
					var connFuncErr error
					defer func() {
						err := recover()
						if err != nil {
							defer recover()
							connFuncErr, _ = err.(error)
							conn.Close()
						}
					}()

					connFuncErr = rio.connFunc(conn, channel)
					return connFuncErr
				}()

				if connErr != nil && rio.logger != nil {
					rio.logger.Println(connErr)
				}
			}
		}
	}
}
