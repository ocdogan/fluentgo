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
	"encoding/json"
	"strings"

	"math"

	"github.com/garyburd/redigo/redis"
	"github.com/ocdogan/fluentgo/config"
	"github.com/ocdogan/fluentgo/lib"
)

type redisOut struct {
	redisIO
	outHandler
	trimSize    int
	channelPath *lib.JsonPath
}

func init() {
	RegisterOut("redis", newRedisOut)
	RegisterOut("redisout", newRedisOut)
}

func newRedisOut(manager InOutManager, params map[string]interface{}) OutSender {
	oh := newOutHandler(manager, params)
	if oh == nil {
		return nil
	}

	rio := newRedisIO(manager.GetLogger(), params)
	if rio == nil {
		return nil
	}

	channelPath := lib.NewJsonPath(rio.channel)
	if channelPath == nil {
		return nil
	}

	cmd := strings.ToUpper(rio.command)
	if !(cmd == lib.Publish || cmd == lib.LPush || cmd == lib.RPush) {
		rio.command = lib.Publish
	}

	ro := &redisOut{
		redisIO:     *rio,
		outHandler:  *oh,
		channelPath: channelPath,
	}

	trimSize, ok := config.ParamAsIntWithLimit(params, "trimSize", 0, math.MaxInt32)
	if ok {
		ro.trimSize = trimSize
	}

	ro.iotype = "REDISOUT"

	ro.runFunc = ro.waitComplete
	ro.connFunc = ro.funcPing

	ro.afterCloseFunc = rio.funcAfterClose

	ro.getDestinationFunc = ro.funcChannel
	ro.sendChunkFunc = ro.funcSendMessagesChunk

	return ro
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
	return "null"
}

func (ro *redisOut) putMessages(messages []string, channel string) {
	if len(messages) == 0 {
		return
	}
	defer recover()

	m := ro.GetManager()
	if m == nil {
		return
	}

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
						msg = lib.BytesToString(lib.Compress([]byte(msg), ro.compressType))
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

func (ro *redisOut) funcSendMessagesChunk(messages []string, channel string) {
	if len(messages) == 0 {
		return
	}
	defer recover()

	if ro.channelPath.IsStatic() {
		channel, _, err := ro.channelPath.Eval(nil, true)
		if err != nil {
			return
		}

		ro.putMessages(messages, channel)
	} else {
		var (
			channel     string
			channelList []string
		)

		channels := make(map[string][]string)

		for _, msg := range messages {
			if msg != "" {
				var data interface{}

				err := json.Unmarshal([]byte(msg), &data)
				if err != nil {
					continue
				}

				channel, _, err = ro.channelPath.Eval(data, true)
				if err != nil {
					continue
				}

				channelList, _ = channels[channel]
				channels[channel] = append(channelList, msg)
			}
		}

		for channel, channelList = range channels {
			ro.putMessages(messages, channel)
		}
	}
}
