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
	"errors"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type sqsIn struct {
	sqsIO
	inHandler
	waitTimeSeconds     int64
	maxNumberOfMessages int64
}

func init() {
	RegisterIn("sqs", newSqsIn)
	RegisterIn("sqsin", newSqsIn)
}

func newSqsIn(manager InOutManager, params map[string]interface{}) InProvider {
	ih := newInHandler(manager, params)
	if ih == nil {
		return nil
	}

	sio := newSqsIO(manager, params)
	if sio == nil {
		return nil
	}

	var (
		f  float64
		ok bool
	)

	waitTimeSeconds := int64(0)
	if f, ok = params["waitTimeSeconds"].(float64); ok {
		waitTimeSeconds = int64(f)
	}
	if !ok || waitTimeSeconds < 0 {
		waitTimeSeconds = 0
	}

	maxNumberOfMessages := int64(10)
	if f, ok = params["maxNumberOfMessages"].(float64); ok {
		maxNumberOfMessages = int64(f)
	} else if !ok {
		maxNumberOfMessages = 10
	}
	if !ok || maxNumberOfMessages < 1 {
		maxNumberOfMessages = 1
	}

	si := &sqsIn{
		sqsIO:               *sio,
		inHandler:           *ih,
		waitTimeSeconds:     waitTimeSeconds,
		maxNumberOfMessages: maxNumberOfMessages,
	}

	si.iotype = "SQSIN"

	si.runFunc = si.funcReceive

	return si
}

func (si *sqsIn) deleteMessage(msg *sqs.Message) error {
	client := si.client
	if client == nil {
		return errors.New("Invalid SQS client.")
	}

	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(si.queueURL),
		ReceiptHandle: aws.String(*msg.ReceiptHandle),
	}

	_, err := client.DeleteMessage(params)
	if err != nil {
		return err
	}
	return nil
}

func (si *sqsIn) funcReceive() {
	defer func() {
		recover()

		l := si.GetLogger()
		if l != nil {
			l.Println("Stoping 'SQSIN'...")
		}
	}()

	l := si.GetLogger()
	if l != nil {
		l.Println("Starting 'SQSIN'...")
	}

	completed := false

	maxMessageSize := si.getMaxMessageSize()

	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(si.queueURL),
		MaxNumberOfMessages: aws.Int64(si.maxNumberOfMessages),
		WaitTimeSeconds:     aws.Int64(si.waitTimeSeconds),
	}

	loop := 0
	for !completed {
		select {
		case <-si.completed:
			completed = true
			si.Close()
			return
		default:
			if completed {
				return
			}

			si.Connect()

			client := si.client
			if client == nil {
				completed = true
				return
			}

			resp, err := client.ReceiveMessage(params)
			if err != nil {
				if l != nil {
					l.Println(err)
				}
				time.Sleep(100 * time.Microsecond)
				continue
			}

			if len(resp.Messages) == 0 {
				time.Sleep(100 * time.Microsecond)
				continue
			}

			var wg sync.WaitGroup
			for _, m := range resp.Messages {
				wg.Add(1)
				go func(sin *sqsIn, msg *sqs.Message, wg *sync.WaitGroup) {
					defer wg.Done()

					err := sin.deleteMessage(msg)
					if err == nil {
						go sin.queueMessage([]byte(*msg.Body), maxMessageSize)
					} else {
						l := sin.GetLogger()
						if l != nil {
							l.Println(err)
						}
					}
				}(si, m, &wg)
			}

			wg.Wait()

			loop++
			if loop%100 == 0 {
				loop = 0
				time.Sleep(time.Millisecond)
			}
		}
	}
}

func (si *sqsIn) Connect() {
	if si.client == nil && si.connFunc == nil {
		si.connFunc()
	}
}
