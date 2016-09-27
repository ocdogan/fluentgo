package inout

import (
	"strings"

	"github.com/garyburd/redigo/redis"
	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/lib/config"
)

type redisChanIn struct {
	redisIO
	inHandler
	pConn *redis.PubSubConn
}

func newRedisChanIn(manager InOutManager, config *config.InOutConfig) *redisChanIn {
	if config == nil {
		return nil
	}

	params := config.GetParamsMap()

	ih := newInHandler(manager, params)
	if ih == nil {
		return nil
	}

	rio := newRedisIO(manager.GetLogger(), params)
	if rio != nil {
		cmd := strings.ToUpper(rio.command)
		if !(cmd == lib.PSubscribe || cmd == lib.Subscribe) {
			if strings.ContainsAny(rio.channel, lib.PSubscribechars) {
				cmd = lib.PSubscribe
			} else {
				cmd = lib.Subscribe
			}
			rio.command = cmd
		}

		ri := &redisChanIn{
			redisIO:   *rio,
			inHandler: *ih,
		}

		ri.iotype = "REDISCHANIN"

		ri.runFunc = ri.funcReceive
		ri.connFunc = ri.funcSubscribe
		ri.afterCloseFunc = ri.funcUnsubscribe

		return ri
	}
	return nil
}

func (ri *redisChanIn) funcUnsubscribe() {
	defer ri.funcAfterClose()

	if ri.channel != "" {
		pConn := ri.pConn
		if pConn != nil {
			defer recover()
			ri.pConn = nil
			pConn.Unsubscribe(ri.channel)
		}
	}
}

func (ri *redisChanIn) funcSubscribe(conn redis.Conn) error {
	var subsErr error
	defer func() {
		err, _ := recover().(error)
		if subsErr == nil {
			subsErr = err
		}
	}()

	psConn := &redis.PubSubConn{Conn: conn}

	if ri.command == lib.PSubscribe {
		subsErr = psConn.PSubscribe(ri.channel)
	} else {
		subsErr = psConn.Subscribe(ri.channel)
	}

	if subsErr == nil {
		ri.pConn = psConn
	} else {
		ri.pConn = nil
	}
	return subsErr
}

func (ri *redisChanIn) funcReceive() {
	defer func() {
		recover()

		l := ri.GetLogger()
		if l != nil {
			l.Println("Stoping 'REDISCHANIN'...")
		}
	}()

	l := ri.GetLogger()
	if l != nil {
		l.Println("Starting 'REDISCHANIN'...")
	}

	completed := false

	compressed := ri.compressed
	maxMessageSize := ri.getMaxMessageSize()

	for !completed {
		select {
		case <-ri.completed:
			completed = true
			ri.Close()
			return
		default:
			if completed {
				return
			}

			ri.Connect()

			pConn := ri.pConn
			if pConn == nil {
				completed = true
				return
			}

			switch m := pConn.Receive().(type) {
			case redis.Message:
				if !completed {
					go ri.queueMessage(m.Data, maxMessageSize, compressed)
				}
			case redis.PMessage:
				if !completed {
					go ri.queueMessage(m.Data, maxMessageSize, compressed)
				}
			case error:
				if !completed {
					l := ri.GetLogger()
					if l != nil {
						l.Println(m)
					}

					if !ri.Processing() {
						ri.completed <- true
						return
					}
				}
			case redis.Subscription:
				l := ri.GetLogger()
				if l != nil {
					l.Printf("Subscribed to '%s' over '%s'\n", m.Channel, strings.ToUpper(m.Kind))
				}
			}
		}
	}
}