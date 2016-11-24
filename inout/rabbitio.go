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
	"math"
	"net"
	"strings"
	"time"

	"github.com/ocdogan/fluentgo/config"
	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/log"
	"github.com/streadway/amqp"
)

type rabbitIO struct {
	port            int
	host            string
	username        string
	password        string
	vhost           string
	exchange        string
	exchangeType    string
	queue           string
	key             string
	tag             string
	contentType     string
	exchangeDeclare bool
	queueBind       bool
	durable         bool // durable
	autoDelete      bool // delete when unused/complete
	exclusive       bool // exclusive
	nowait          bool // no-wait
	internal        bool
	autoAck         bool
	noLocal         bool
	connected       bool
	timeout         time.Duration
	connFunc        func(*amqp.Connection, *amqp.Channel) error
	conn            *amqp.Connection
	channel         *amqp.Channel
	logger          log.Logger
}

func newRabbitIO(logger log.Logger, params map[string]interface{}) *rabbitIO {
	if params == nil {
		return nil
	}

	queue, ok := config.ParamAsString(params, "queue")
	if !ok || queue == "" {
		return nil
	}

	key, ok := config.ParamAsString(params, "key")
	if !ok || key == "" {
		return nil
	}

	exchange, ok := config.ParamAsString(params, "exchange")
	if !ok || exchange == "" {
		return nil
	}

	exchangeType, ok := config.ParamAsString(params, "exchangeType")
	if !ok || exchangeType == "" {
		return nil
	}

	host, ok := config.ParamAsString(params, "host")
	if !ok || host == "" {
		return nil
	}

	tag, _ := config.ParamAsString(params, "tag")

	port, ok := config.ParamAsInt(params, "port")
	if !ok {
		port = 5672
	} else {
		port = lib.MaxInt(math.MaxInt16, lib.MinInt(0, port))
	}

	timeout, _ := config.ParamAsDuration(params, "timeout")
	timeout = lib.MaxDuration(60, lib.MinDuration(0, timeout))

	vhost, _ := config.ParamAsString(params, "vhost")

	var password string
	username, ok := config.ParamAsString(params, "username")
	if ok && username != "" {
		password, _ = config.ParamAsString(params, "password")
	}

	contentType, ok := config.ParamAsString(params, "contentType")
	if ok && contentType != "" {
		contentType = strings.ToLower(contentType)
	}

	exchangeDeclare, _ := config.ParamAsBool(params, "exchangeDeclare")
	queueBind, _ := config.ParamAsBool(params, "queueBind")
	durable, _ := config.ParamAsBool(params, "durable")
	autoDelete, _ := config.ParamAsBool(params, "autoDelete")
	exclusive, _ := config.ParamAsBool(params, "exclusive")
	nowait, _ := config.ParamAsBool(params, "nowait")
	internal, _ := config.ParamAsBool(params, "internal")
	autoAck, _ := config.ParamAsBool(params, "autoAck")
	noLocal, _ := config.ParamAsBool(params, "noLocal")

	rio := &rabbitIO{
		host:            host,
		port:            port,
		vhost:           vhost,
		username:        username,
		password:        password,
		exchangeDeclare: exchangeDeclare,
		exchange:        exchange,
		exchangeType:    exchangeType,
		queueBind:       queueBind,
		queue:           queue,
		key:             key,
		tag:             tag,
		durable:         durable,
		autoDelete:      autoDelete,
		exclusive:       exclusive,
		nowait:          nowait,
		internal:        internal,
		autoAck:         autoAck,
		noLocal:         noLocal,
		timeout:         timeout,
		contentType:     contentType,
		logger:          logger,
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

func (rio *rabbitIO) funcSubscribe(conn *amqp.Connection, channel *amqp.Channel) error {
	var err error
	defer func() {
		subsErr, _ := recover().(error)
		if err == nil {
			err = subsErr
		}
	}()

	if rio.channel == nil {
		return nil
	}

	if rio.exchangeDeclare {
		err = rio.channel.ExchangeDeclare(
			rio.exchange,     // name of the exchange
			rio.exchangeType, // type
			rio.durable,      // durable
			rio.autoDelete,   // delete when complete
			rio.internal,     // internal
			rio.nowait,       // noWait
			nil,              // arguments
		)

		if err != nil {
			return err
		}
	}

	var queue amqp.Queue
	queue, err = rio.channel.QueueDeclare(
		rio.queue,      // name of the queue
		rio.durable,    // durable
		rio.autoDelete, // delete when unused/complete
		rio.exclusive,  // exclusive
		rio.nowait,     // noWait
		nil,            // arguments
	)

	if err != nil {
		return err
	}

	if rio.queueBind {
		err = rio.channel.QueueBind(
			queue.Name,   // name of the queue
			rio.key,      // bindingKey
			rio.exchange, // sourceExchange
			rio.nowait,   // noWait
			nil,          // arguments
		)

		if err != nil {
			return err
		}
	}

	return err
}
