package inout

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/ocdogan/fluentgo/lib/config"
)

type sqsIO struct {
	awsIO
	queueURL   string
	attributes map[string]*sqs.MessageAttributeValue
	client     *sqs.SQS
	connFunc   func() *sqs.SQS
}

func newSqsIO(manager InOutManager, config *config.InOutConfig) *sqsIO {
	if config == nil {
		return nil
	}

	var (
		ok            bool
		pName, pValue string
	)

	awsio := newAwsIO(manager, config)
	if awsio == nil {
		return nil
	}

	params := config.GetParamsMap()
	attributes := make(map[string]*sqs.MessageAttributeValue, 0)

	for k, v := range params {
		if v != nil && strings.HasPrefix(k, "attribute.") {
			pName = strings.TrimSpace(k[len("attribute."):])
			if pName != "" {
				pValue, ok = v.(string)
				if ok {
					pValue = strings.TrimSpace(pValue)
					attributes[pName] = &sqs.MessageAttributeValue{
						StringValue: aws.String(strings.TrimSpace(pValue)),
					}
				}
			}
		}
	}

	var (
		queueURL string
	)

	queueURL, ok = params["queueURL"].(string)
	if ok {
		queueURL = strings.TrimSpace(queueURL)
	}
	if queueURL == "" {
		return nil
	}
	sio := &sqsIO{
		awsIO:      *awsio,
		queueURL:   queueURL,
		attributes: attributes,
	}

	sio.connFunc = sio.funcGetClient

	return sio
}

func (sio *sqsIO) funcGetClient() *sqs.SQS {
	if sio.client == nil {
		defer recover()
		sio.client = sqs.New(session.New(), sio.getAwsConfig())
	}
	return sio.client
}
