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
	"time"

	"github.com/optiopay/kafka"
)

type kafkaIn struct {
	inHandler
	kafkaIO
	consumer *kafka.Consumer
}

func init() {
	RegisterIn("kafka", newKafkaIn)
	RegisterIn("kafkain", newKafkaIn)
}

func newKafkaIn(manager InOutManager, params map[string]interface{}) InProvider {
	ih := newInHandler(manager, params)
	if ih == nil {
		return nil
	}

	kio := newKafkaIO(manager, ih.id, params)
	if kio == nil {
		return nil
	}

	kin := &kafkaIn{
		inHandler: *ih,
		kafkaIO:   *kio,
	}

	kin.runFunc = kin.funcReceive
	kin.afterCloseFunc = kin.funcAfterClose

	return kin
}

func (kin *kafkaIn) funcAfterClose() {
	if kin.broker != nil {
		defer recover()

		broker := *kin.broker
		kin.broker = nil
		kin.consumer = nil

		broker.Close()
	}
}

func (kin *kafkaIn) funcReceive() {
	defer kin.InformStop()
	kin.InformStart()

	consumeCompleted := make(chan bool)
	go kin.consume(consumeCompleted)

	<-consumeCompleted
}

func (kin *kafkaIn) consume(consumeCompleted chan bool) {
	defer func() {
		recover()
		close(consumeCompleted)
	}()

	loop := 0
	completed := false

	l := kin.GetLogger()
	maxMessageSize := kin.getMaxMessageSize()

	for !completed {
		select {
		case <-consumeCompleted:
			completed = true
			kin.Close()
			return
		case <-kin.completed:
			completed = true
			kin.Close()
			return
		default:
			if completed {
				return
			}

			kin.Connect()

			consumer := kin.consumer
			if consumer == nil {
				completed = true
				return
			}

			msg, err := (*consumer).Consume()
			if err != nil {
				if err != kafka.ErrNoData {
					l.Printf("Cannot consume KAFKAIN %q topic message: %s\n", kin.topic, err)
				} else {
					time.Sleep(10 * time.Microsecond)
				}
				continue
			}

			if msg != nil && len(msg.Value) > 0 {
				kin.queueMessage(msg.Value, maxMessageSize)
			}

			loop++
			if loop%100 == 0 {
				loop = 0
				time.Sleep(time.Millisecond)
			}
		}
	}
}

func (kin *kafkaIn) Connect() error {
	if kin.broker == nil {
		kin.consumer = nil

		err := kin.dial()
		if err != nil {
			l := kin.GetLogger()
			if l != nil {
				l.Printf("Failed to create KAFKAIN broker: %s\n", err)
			}
			return err
		}
	}

	if kin.consumer == nil {
		conf := kafka.NewConsumerConf(kin.topic, kin.partition)
		conf.StartOffset = int64(kin.offset)

		consumer, err := kin.broker.Consumer(conf)
		if err != nil {
			l := kin.GetLogger()
			if l != nil {
				l.Printf("Cannot create KAFKAIN consumer for '%s:%d', error: %s\n", kin.topic, kin.partition, err)
			}
			return err
		}

		if consumer == nil {
			l := kin.GetLogger()
			if l != nil {
				l.Printf("Cannot create KAFKAIN consumer for '%s:%d'\n", kin.topic, kin.partition)
			}
			return err
		}

		kin.consumer = &consumer
	}
	return nil
}
