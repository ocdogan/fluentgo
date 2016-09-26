package inout

import (
	"errors"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/ocdogan/fluentgo/lib/config"
)

type sqsIn struct {
	sqsIO
	inHandler
	waitTimeSeconds     int64
	maxNumberOfMessages int64
}

func newSqsIn(manager InOutManager, config *config.InOutConfig) *sqsIn {
	if config == nil {
		return nil
	}

	params := config.GetParamsMap()

	ih := newInHandler(manager, params)
	if ih == nil {
		return nil
	}

	sio := newSqsIO(manager, config)
	if sio != nil {
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
	return nil
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

					err := si.deleteMessage(msg)
					if err == nil {
						go sin.queueMessage([]byte(*msg.Body), maxMessageSize, false)
					} else {
						l := si.GetLogger()
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
