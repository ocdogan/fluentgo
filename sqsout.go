package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type sqsOut struct {
	outHandler
	sqsIO
	delaySeconds int64
}

func newSqsOut(manager InOutManager, config *inOutConfig) *sqsOut {
	if config == nil {
		return nil
	}

	sio := newSqsIO(manager, config)
	if sio == nil {
		return nil
	}

	params := config.getParamsMap()

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
