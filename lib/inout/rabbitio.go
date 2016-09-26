package inout

import (
	"math"
	"net"
	"strings"
	"time"

	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/lib/log"
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
	compressed      bool
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
		contentType  string
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
		port = lib.MaxInt(math.MaxInt16, lib.MinInt(0, int(f)))
	}

	f, ok = params["timeout"].(float64)
	if ok {
		timeout = time.Duration(lib.MaxInt(60, lib.MinInt(0, int(f))))
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

	contentType, ok = params["contentType"].(string)
	if ok {
		contentType = strings.ToLower(strings.TrimSpace(contentType))
	}

	var (
		exchangeDeclare bool
		queueBind       bool
		durable         bool // durable
		autoDelete      bool // delete when unused/complete
		exclusive       bool // exclusive
		nowait          bool // no-wait
		autoAck         bool
		internal        bool
		noLocal         bool
		compressed      bool
	)

	exchangeDeclare, ok = params["exchangeDeclare"].(bool)
	queueBind, ok = params["queueBind"].(bool)
	durable, ok = params["durable"].(bool)
	autoDelete, ok = params["autoDelete"].(bool)
	exclusive, ok = params["exclusive"].(bool)
	nowait, ok = params["nowait"].(bool)
	internal, ok = params["internal"].(bool)
	autoAck, ok = params["autoAck"].(bool)
	noLocal, ok = params["noLocal"].(bool)

	compressed, ok = params["compressed"].(bool)

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
		compressed:      compressed,
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
