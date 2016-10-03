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
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/ocdogan/fluentgo/lib"
)

type redisListIn struct {
	redisIO
	inHandler
	blockingCmd bool
}

func newRedisListIn(manager InOutManager, params map[string]interface{}) *redisListIn {
	ih := newInHandler(manager, params)
	if ih == nil {
		return nil
	}

	rio := newRedisIO(manager.GetLogger(), params)
	if rio != nil {
		cmd := strings.ToUpper(rio.command)
		if !(cmd == lib.RPop || cmd == lib.LPop || cmd == lib.BrPop || cmd == lib.BlPop) {
			rio.command = lib.LPop
		}

		ri := &redisListIn{
			redisIO:     *rio,
			inHandler:   *ih,
			blockingCmd: rio.command[0] == 'B',
		}

		ri.iotype = "REDISLISTIN"

		ri.runFunc = ri.funcReceive
		ri.connFunc = ri.funcPing

		return ri
	}
	return nil
}

func (ri *redisListIn) funcPing(conn redis.Conn) error {
	return ri.ping(conn)
}

func (ri *redisListIn) funcReceive() {
	defer func() {
		recover()

		l := ri.GetLogger()
		if l != nil {
			l.Println("Stoping 'REDISLISTIN'...")
		}
	}()

	l := ri.GetLogger()
	if l != nil {
		l.Println("Starting 'REDISLISTIN'...")
	}

	completed := false

	maxMessageSize := ri.getMaxMessageSize()

	loop := 0
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

			conn := ri.conn
			if conn == nil {
				completed = true
				return
			}

			var (
				rep interface{}
				err error
			)

			rep, err = conn.Do(ri.command, ri.channel)

			if err != nil {
				if l != nil {
					l.Println(err)
				}
				time.Sleep(100 * time.Microsecond)
				continue
			}

			switch m := rep.(type) {
			case []byte:
				if !completed {
					go ri.queueMessage(m, maxMessageSize)
				}
			case string:
				if !completed {
					if ri.blockingCmd {
						m = strings.SplitN(m, "\n", 2)[1]
					}
					go ri.queueMessage([]byte(m), maxMessageSize)
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
			}

			loop++
			if loop%100 == 0 {
				loop = 0
				time.Sleep(time.Millisecond)
			}
		}
	}
}
