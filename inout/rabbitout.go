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
	"github.com/ocdogan/fluentgo/lib"
	"github.com/streadway/amqp"
)

type rabbitOut struct {
	rabbitIO
	outHandler
	mandatory    bool
	immediate    bool
	exchangePath *lib.JsonPath
	queuePath    *lib.JsonPath
}

func init() {
	RegisterOut("rabbit", newRabbitOut)
	RegisterOut("rabbitout", newRabbitOut)
}

func newRabbitOut(manager InOutManager, params map[string]interface{}) OutSender {
	oh := newOutHandler(manager, params)
	if oh == nil {
		return nil
	}

	rio := newRabbitIO(manager.GetLogger(), params)
	if rio == nil {
		return nil
	}

	exchangePath := lib.NewJsonPath(rio.exchange)
	if exchangePath != nil {
		return nil
	}

	queuePath := lib.NewJsonPath(rio.queue)
	if queuePath == nil {
		return nil
	}

	mandatory, _ := params["mandatory"].(bool)
	immediate, _ := params["immediate"].(bool)

	ro := &rabbitOut{
		rabbitIO:     *rio,
		outHandler:   *oh,
		mandatory:    mandatory,
		immediate:    immediate,
		exchangePath: exchangePath,
		queuePath:    queuePath,
	}

	ro.iotype = "RABBITOUT"

	ro.runFunc = ro.funcWait
	ro.connFunc = ro.funcSubscribe

	ro.afterCloseFunc = rio.funcAfterClose

	ro.getDestinationFunc = ro.funcChannel
	ro.sendChunkFunc = ro.funcPutMessages

	return ro
}

func (ro *rabbitOut) funcChannel() string {
	return "null"
}

func (ro *rabbitOut) putMessages(messages []string, exchange, queue string) {
	if len(messages) == 0 {
		return
	}
	defer recover()

	m := ro.GetManager()
	if m == nil {
		return
	}

	var (
		err     error
		channel *amqp.Channel
	)

	var body []byte
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

				channel = ro.channel
				if channel != nil {
					body = []byte(msg)
					if ro.compressed {
						body = lib.Compress(body, ro.compressType)
					}

					if len(body) > 0 {
						sendErr = channel.Publish(
							exchange,     // exchange
							queue,        // routing key
							ro.mandatory, // mandatory
							ro.immediate, // immediate
							amqp.Publishing{
								ContentType: ro.contentType,
								Body:        body,
							})
					}
				}
				return sendErr
			}()
		}
	}
}

func (ro *rabbitOut) funcPutMessages(messages []string, channel string) {
	if len(messages) == 0 {
		return
	}
	defer recover()

	exchanges := ro.groupMessages(messages, ro.exchangePath, ro.queuePath)
	if len(exchanges) == 0 {
		return
	}

	for exchange, exchangeMap := range exchanges {
		for queue, msgs := range exchangeMap {
			ro.putMessages(msgs, exchange, queue)
		}
	}
}

func (ro *rabbitOut) funcWait() {
	defer func() {
		recover()
		l := ro.GetLogger()
		if l != nil {
			l.Println("Stoping 'RABBITOUT'...")
		}
	}()

	l := ro.GetLogger()
	if l != nil {
		l.Println("Starting 'RABBITOUT'...")
	}

	<-ro.completed
}
