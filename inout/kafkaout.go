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
	"fmt"

	"github.com/ocdogan/fluentgo/lib"
	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
)

type kafkaOut struct {
	outHandler
	kafkaIO
	producer  *kafka.Producer
	topicPath *lib.JsonPath
}

func init() {
	RegisterOut("kafka", newKafkaOut)
	RegisterOut("kafkaout", newKafkaOut)
}

func newKafkaOut(manager InOutManager, params map[string]interface{}) OutSender {
	oh := newOutHandler(manager, params)
	if oh == nil {
		return nil
	}

	kio := newKafkaIO(manager, oh.id, params)
	if kio == nil {
		return nil
	}

	topicPath := lib.NewJsonPath(kio.topic)
	if topicPath == nil {
		return nil
	}

	ko := &kafkaOut{
		outHandler: *oh,
		kafkaIO:    *kio,
		topicPath:  topicPath,
	}

	ko.iotype = "KAFKAOUT"

	ko.runFunc = ko.funcRunAndWait
	ko.afterCloseFunc = ko.funcAfterClose
	ko.getDestinationFunc = ko.funcGetObjectName
	ko.sendChunkFunc = ko.funcPutMessages

	return ko
}

func (ko *kafkaOut) funcAfterClose() {
	if ko.broker != nil {
		defer recover()

		broker := *ko.broker
		ko.broker = nil
		ko.producer = nil

		broker.Close()
	}
}

func (ko *kafkaOut) funcGetObjectName() string {
	return "null"
}

func (ko *kafkaOut) putMessages(messages []string, topic string) {
	if len(messages) == 0 {
		return
	}
	defer recover()

	err := ko.Connect()
	if err != nil || ko.producer == nil {
		return
	}

	l := ko.GetLogger()
	producer := *ko.producer

	for _, msg := range messages {
		if msg != "" {
			data := []byte(msg)
			if ko.compressed {
				data = lib.Compress(data, ko.compressType)
			}

			kmsg := &proto.Message{Value: data}
			if _, err := producer.Produce(topic, ko.partition, kmsg); err != nil && l != nil {
				l.Printf("Cannot produce KAFKAOUT message to %s:%d: %s", ko.topic, ko.partition, err)
			}
		}
	}
}

func (ko *kafkaOut) funcPutMessages(messages []string, topic string) {
	if len(messages) == 0 {
		return
	}
	defer recover()

	if ko.topicPath.IsStatic() {
		topic, _, err := ko.topicPath.Eval(nil, true)
		if err != nil {
			return
		}

		ko.putMessages(messages, topic)
	} else {
		var (
			topic     string
			topicList []string
		)

		topics := make(map[string][]string)

		for _, msg := range messages {
			if msg != "" {
				var data interface{}

				err := json.Unmarshal([]byte(msg), &data)
				if err != nil {
					continue
				}

				topic, _, err = ko.topicPath.Eval(data, true)
				if err != nil {
					continue
				}

				topicList, _ = topics[topic]
				topics[topic] = append(topicList, msg)
			}
		}

		for topic, topicList = range topics {
			ko.putMessages(messages, topic)
		}
	}
}

func (ko *kafkaOut) funcRunAndWait() {
	defer func() {
		recover()
		l := ko.GetLogger()
		if l != nil {
			l.Println("Stoping 'KAFKAOUT'...")
		}
	}()

	l := ko.GetLogger()
	if l != nil {
		l.Println("Starting 'KAFKAOUT'...")
	}

	<-ko.completed
}

func (ko *kafkaOut) Connect() error {
	if ko.broker == nil {
		ko.producer = nil

		err := ko.dial()
		if err != nil {
			l := ko.GetLogger()
			if l != nil {
				l.Printf("Failed to create KAFKAOUT broker: %s\n", err)
			}
			return err
		}
	}

	if ko.producer == nil {
		producer := ko.broker.Producer(kafka.NewProducerConf())
		if producer == nil {
			err := fmt.Errorf("Cannot create KAFKAOUT producer for '%s:%d'", ko.topic, ko.partition)

			l := ko.GetLogger()
			if l != nil {
				l.Println(err)
			}
			return err
		}

		ko.producer = &producer
	}
	return nil
}
