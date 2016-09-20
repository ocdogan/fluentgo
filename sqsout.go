package main

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type sqsOut struct {
	outHandler
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
	region          string
	queueURL        string
	disableSSL      bool
	maxRetries      int
	logLevel        uint
	delaySeconds    int64
	client          *sqs.SQS
}

func newSqsOut(manager InOutManager, config *inOutConfig) *sqsOut {
	if config == nil {
		return nil
	}

	params := make(map[string]interface{}, len(config.Params))
	for _, p := range config.Params {
		params[p.Name] = p.Value
	}

	var (
		accessKeyID     string
		secretAccessKey string
		token           string
		region          string
		queueURL        string
		ok              bool
	)

	queueURL, ok = params["queueURL"].(string)
	if ok {
		queueURL = strings.TrimSpace(queueURL)
	}
	if queueURL == "" {
		return nil
	}

	region, ok = params["region"].(string)
	if ok {
		region = strings.TrimSpace(region)
	}
	if region == "" {
		return nil
	}

	accessKeyID, ok = params["accessKeyID"].(string)
	if ok {
		accessKeyID = strings.TrimSpace(accessKeyID)
		if accessKeyID != "" {
			secretAccessKey, ok = params["secretAccessKey"].(string)
			if ok {
				secretAccessKey = strings.TrimSpace(secretAccessKey)
			}
		}
	}

	token, ok = params["sessionToken"].(string)
	if ok {
		token = strings.TrimSpace(token)
	}

	var (
		f            float64
		disableSSL   bool
		delaySeconds int64
	)

	if disableSSL, ok = params["disableSSL"].(bool); !ok {
		disableSSL = false
	}

	delaySeconds = 0
	if f, ok = params["delaySeconds"].(float64); ok {
		delaySeconds = int64(f)
	}
	if !ok || delaySeconds < 0 {
		delaySeconds = 0
	}

	maxRetries := 0
	if f, ok = params["maxRetries"].(float64); ok {
		maxRetries = int(f)
	}
	if !ok || maxRetries < 1 {
		maxRetries = 1
	}

	logLevel := 0
	if f, ok = params["logLevel"].(float64); ok {
		logLevel = maxInt(int(f), 0)
	}

	oh := newOutHandler(manager, params)
	if oh == nil {
		return nil
	}

	sqso := &sqsOut{
		outHandler:      *oh,
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		sessionToken:    token,
		region:          region,
		queueURL:        queueURL,
		disableSSL:      disableSSL,
		maxRetries:      maxRetries,
		logLevel:        uint(logLevel),
		delaySeconds:    delaySeconds,
	}

	sqso.iotype = "SQSOUT"

	sqso.runFunc = sqso.funcRunAndWait
	sqso.afterCloseFunc = sqso.funcAfterClose
	sqso.getDestinationFunc = sqso.funcGetObjectName
	sqso.sendChunkFunc = sqso.funcPutMessages

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

				client.SendMessage(params)
			}
		}
	}
}

func (sqso *sqsOut) getClient() *sqs.SQS {
	if sqso.client == nil {
		defer recover()

		cfg := aws.NewConfig().
			WithRegion(sqso.region).
			WithDisableSSL(sqso.disableSSL).
			WithMaxRetries(sqso.maxRetries)

		if sqso.accessKeyID != "" && sqso.secretAccessKey != "" {
			creds := credentials.NewStaticCredentials(sqso.accessKeyID, sqso.secretAccessKey, sqso.sessionToken)
			cfg = cfg.WithCredentials(creds)
		}

		if sqso.logLevel > 0 {
			l := sqso.GetLogger()
			if l != nil {
				cfg.Logger = l
				cfg.LogLevel = aws.LogLevel(aws.LogLevelType(sqso.logLevel))
			}
		}

		sqso.client = sqs.New(session.New(), cfg)
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
