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

	"github.com/streadway/amqp"
)

type rabbitIn struct {
	rabbitIO
	inHandler
	deliveries <-chan amqp.Delivery
}

func init() {
	RegisterIn("rabbit", newRabbitIn)
	RegisterIn("rabbitin", newRabbitIn)
}

func newRabbitIn(manager InOutManager, params map[string]interface{}) InProvider {
	ih := newInHandler(manager, params)
	if ih == nil {
		return nil
	}

	rio := newRabbitIO(manager.GetLogger(), params)
	if rio != nil {
		ri := &rabbitIn{
			rabbitIO:  *rio,
			inHandler: *ih,
		}

		ri.iotype = "RABBITIN"

		ri.runFunc = ri.funcReceive
		ri.connFunc = ri.funcSubscribe
		ri.afterCloseFunc = ri.funcUnsubscribe

		return ri
	}
	return nil
}

func (ri *rabbitIn) funcUnsubscribe() {
	defer ri.funcAfterClose()

	if ri.connected {
		channel := ri.channel
		if channel != nil {
			defer recover()
			channel.Cancel(ri.tag, true)
		}
	}
}

func (ri *rabbitIn) funcSubscribe(conn *amqp.Connection, channel *amqp.Channel) error {
	var err error
	defer func() {
		subsErr, _ := recover().(error)
		if err == nil {
			err = subsErr
		}
	}()

	err = ri.funcSubscribe(conn, channel)
	if err != nil {
		return err
	}

	ri.deliveries, err = channel.Consume(
		ri.queue,     // name
		ri.tag,       // consumerTag,
		ri.autoAck,   // noAck
		ri.exclusive, // exclusive
		ri.noLocal,   // noLocal
		ri.nowait,    // noWait
		nil,          // arguments
	)

	return err
}

func (ri *rabbitIn) validContentType(contentType string) bool {
	return ri.contentType == "" || ri.contentType == "*" ||
		ri.contentType == strings.ToLower(contentType)
}

func (ri *rabbitIn) funcReceive() {
	defer func() {
		recover()

		l := ri.GetLogger()
		if l != nil {
			l.Println("Stoping 'RABBITIN'...")
		}
	}()

	l := ri.GetLogger()
	if l != nil {
		l.Println("Starting 'RABBITIN'...")
	}

	completed := false

	maxMessageSize := ri.getMaxMessageSize()

	for !completed {
		select {
		case <-ri.completed:
			completed = true
			ri.Close()
			return
		case msg := <-ri.deliveries:
			if completed {
				return
			}

			ri.Connect()

			msg.Ack(false)
			if len(msg.Body) > 0 && ri.validContentType(msg.ContentType) {
				go ri.queueMessage(msg.Body, maxMessageSize)
			}
		}
	}
}
