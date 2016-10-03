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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/ocdogan/fluentgo/lib/config"
)

type sqsOut struct {
	outHandler
	sqsIO
	delaySeconds int64
}

func newSqsOut(manager InOutManager, config *config.InOutConfig) *sqsOut {
	if config == nil {
		return nil
	}

	params := config.GetParamsMap()

	sio := newSqsIO(manager, params)
	if sio == nil {
		return nil
	}

	delaySeconds := int64(0)
	if f, ok := params["delaySeconds"].(float64); ok {
		delaySeconds = int64(f)
	}
	if delaySeconds < 0 {
		delaySeconds = 0
	}

	oh := newOutHandler(manager, params)
	if oh == nil {
		return nil
	}

	sqso := &sqsOut{
		outHandler:   *oh,
		sqsIO:        *sio,
		delaySeconds: delaySeconds,
	}

	sqso.iotype = "SQSOUT"

	sqso.runFunc = sqso.funcRunAndWait
	sqso.afterCloseFunc = sqso.funcAfterClose
	sqso.getDestinationFunc = sqso.funcGetObjectName
	sqso.sendChunkFunc = sqso.funcPutMessages
	sqso.getLoggerFunc = sqso.GetLogger

	return sqso
}

func (sqso *sqsOut) funcAfterClose() {
	if sqso != nil {
		sqso.client = nil
	}
}

func (sqso *sqsOut) funcGetObjectName() string {
	return "null"
}

func (sqso *sqsOut) funcPutMessages(messages []string, indexName string) {
	if len(messages) > 0 {
		defer recover()

		client := sqso.getClient()
		if client == nil {
			return
		}

		for _, msg := range messages {
			if msg != "" {
				params := &sqs.SendMessageInput{
					MessageBody:  aws.String(msg),
					QueueUrl:     aws.String(sqso.queueURL),
					DelaySeconds: aws.Int64(sqso.delaySeconds),
				}

				if len(sqso.attributes) > 0 {
					params.MessageAttributes = sqso.attributes
				}

				client.SendMessage(params)
			}
		}
	}
}

func (sqso *sqsOut) getClient() *sqs.SQS {
	if sqso.client == nil && sqso.connFunc == nil {
		return sqso.connFunc()
	}
	return sqso.client
}

func (sqso *sqsOut) funcRunAndWait() {
	defer func() {
		recover()
		l := sqso.GetLogger()
		if l != nil {
			l.Println("Stoping 'SQSOUT'...")
		}
	}()

	l := sqso.GetLogger()
	if l != nil {
		l.Println("Starting 'SQSOUT'...")
	}

	<-sqso.completed
}
