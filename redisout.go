package main

import (
	"strings"

	"github.com/garyburd/redigo/redis"
)

type redisOut struct {
	redisIO
	outHandler
	trimSize int
}

func newRedisOut(manager InOutManager, config *inOutConfig) *redisOut {
	if config == nil {
		return nil
	}

	params := make(map[string]interface{}, len(config.Params))
	for _, p := range config.Params {
		params[p.Name] = p.Value
	}

	oh := newOutHandler(manager, params)
	if oh == nil {
		return nil
	}

	rio := newRedisIO(manager.GetLogger(), params)
	if rio != nil {
		cmd := strings.ToUpper(rio.command)
		if !(cmd == publish || cmd == lpush || cmd == rpush) {
			rio.command = publish
		}

		ro := &redisOut{
			redisIO:    *rio,
			outHandler: *oh,
		}

		f, ok := params["trimSize"].(float64)
		if ok {
			ro.trimSize = maxInt(0, int(f))
		}

		ro.iotype = "REDISOUT"

		ro.runFunc = ro.funcWait
		ro.connFunc = ro.funcPing

		ro.afterCloseFunc = rio.funcAfterClose

		ro.getDestinationFunc = ro.funcChannel
		ro.sendChunkFunc = ro.funcSendMessagesChunk

		return ro
	}
	return nil
}

func (ro *redisOut) funcPing(conn redis.Conn) error {
	var err error
	defer func() {
		pingErr, _ := recover().(error)
		if err == nil {
			err = pingErr
		}
	}()

	if conn != nil {
		err = conn.Send("PING")
	}
	return err
}

func (ro *redisOut) funcChannel() string {
	return ro.channel
}

func (ro *redisOut) funcSendMessagesChunk(messages []string, channel string) {
	if len(messages) > 0 {
		m := ro.GetManager()
		if m == nil {
			return
		}

		defer recover()

		var (
			err  error
			conn redis.Conn
		)

		for _, msg := range messages {
			if !(err == nil && ro.Processing() && m.Processing()) {
				break
			}

			if msg != "" {
				err = func() error {
					var sendErr error
					defer func() {
						sendErr, _ = recover().(error)
					}()

					ro.Connect()

					conn = ro.conn
					if conn != nil {
						if ro.compressed {
							msg = string(compress([]byte(msg)))
						}

						sendErr = conn.Send(ro.command, channel, msg)

						if sendErr == nil && ro.trimSize > 0 {
							func() {
								defer recover()
								conn.Send("LTRIM", channel, 0, ro.trimSize)
							}()
						}
					}
					return sendErr
				}()
			}
		}
	}
}

func (ro *redisOut) funcWait() {
	defer func() {
		recover()
		l := ro.GetLogger()
		if l != nil {
			l.Println("Stoping 'REDISOUT'...")
		}
	}()

	l := ro.GetLogger()
	if l != nil {
		l.Println("Starting 'REDISOUT'...")
	}

	<-ro.completed
}
