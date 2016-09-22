package main

import (
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

type redisListIn struct {
	redisIO
	inHandler
	blockingCmd bool
}

func newRedisListIn(manager InOutManager, config *inOutConfig) *redisListIn {
	if config == nil {
		return nil
	}

	params := config.getParamsMap()

	ih := newInHandler(manager, params)
	if ih == nil {
		return nil
	}

	rio := newRedisIO(manager.GetLogger(), params)
	if rio != nil {
		cmd := strings.ToUpper(rio.command)
		if !(cmd == rpop || cmd == lpop || cmd == brpop || cmd == blpop) {
			rio.command = lpop
		}

		ri := &redisListIn{
			redisIO:     *rio,
			inHandler:   *ih,
			blockingCmd: rio.command[0] == "BLOCKING"[0],
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

	compressed := ri.compressed
	maxMessageSize := minInt(InvalidMessageSize, maxInt(-1, ri.manager.GetMaxMessageSize()))

	loop := 0
	for {
		select {
		case <-ri.completed:
			completed = true
			ri.Close()
			continue
		default:
			if completed {
				break
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
					go ri.queueMessage(m, maxMessageSize, compressed)
				}
			case string:
				if !completed {
					if ri.blockingCmd {
						m = strings.SplitN(m, "\n", 2)[1]
					}
					go ri.queueMessage([]byte(m), maxMessageSize, compressed)
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

		if completed {
			return
		}
	}
}
