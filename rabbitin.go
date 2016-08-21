package main

import (
	"strings"

	"github.com/streadway/amqp"
)

type rabbitIn struct {
	rabbitIO
	inHandler
	deliveries <-chan amqp.Delivery
}

func newRabbitIn(manager InOutManager, config *inOutConfig) *rabbitIn {
	if config == nil {
		return nil
	}

	params := make(map[string]interface{}, len(config.Params))
	for _, p := range config.Params {
		params[p.Name] = p.Value
	}

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
	defer recover()

	completed := false

	compressed := ri.compressed
	maxMessageSize := minInt(InvalidMessageSize, maxInt(-1, ri.manager.GetMaxMessageSize()))

	for {
		select {
		case <-ri.completed:
			completed = true
			ri.Close()
			continue
		case msg := <-ri.deliveries:
			if completed {
				break
			}

			ri.Connect()

			msg.Ack(false)
			if len(msg.Body) > 0 && ri.validContentType(msg.ContentType) {
				go ri.queueMessage(msg.Body, maxMessageSize, compressed)
			}
		}

		if completed {
			return
		}
	}
}
