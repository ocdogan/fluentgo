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
	"strings"

	"github.com/garyburd/redigo/redis"
	"github.com/ocdogan/fluentgo/lib"
)

type redisChanIn struct {
	redisIO
	inHandler
	pConn *redis.PubSubConn
}

func init() {
	RegisterIn("redischan", newRedisChanIn)
	RegisterIn("redischanin", newRedisChanIn)
}

func newRedisChanIn(manager InOutManager, params map[string]interface{}) InProvider {
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
		ri.connFunc = ri.funcInitPubSub
		ri.afterCloseFunc = ri.funcClosePubSub

		return ri
	}
	return nil
}

func (ri *redisChanIn) funcClosePubSub() {
	defer ri.funcAfterClose()

	pConn := ri.pConn
	if pConn != nil {
		defer recover()

		ri.pConn = nil
		if ri.channel != "" {
			pConn.Unsubscribe(ri.channel)
		}
	}
}

func (ri *redisChanIn) funcInitPubSub(conn redis.Conn) error {
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
					go ri.queueMessage(m.Data, maxMessageSize)
				}
			case redis.PMessage:
				if !completed {
					go ri.queueMessage(m.Data, maxMessageSize)
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

					conn := pConn.Conn
					if conn != nil && conn.Err() == nil {
						ri.tryToCloseConn(conn)
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
