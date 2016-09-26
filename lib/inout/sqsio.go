package inout

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/lib/config"
	"github.com/ocdogan/fluentgo/lib/log"
)

type sqsIO struct {
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
	region          string
	queueURL        string
	disableSSL      bool
	maxRetries      int
	logLevel        uint
	attributes      map[string]*sqs.MessageAttributeValue
	client          *sqs.SQS
	connFunc        func() *sqs.SQS
	getLoggerFunc   func() log.Logger
}

func newSqsIO(manager InOutManager, config *config.InOutConfig) *sqsIO {
	if config == nil {
		return nil
	}

	var (
		ok            bool
		pName, pValue string
	)

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
		accessKeyID     string
		secretAccessKey string
		token           string
		region          string
		queueURL        string
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
		f          float64
		disableSSL bool
	)

	if disableSSL, ok = params["disableSSL"].(bool); !ok {
		disableSSL = false
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
		logLevel = lib.MaxInt(int(f), 0)
	}

	sio := &sqsIO{
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		sessionToken:    token,
		region:          region,
		queueURL:        queueURL,
		disableSSL:      disableSSL,
		maxRetries:      maxRetries,
		logLevel:        uint(logLevel),
		attributes:      attributes,
	}

	sio.connFunc = sio.funcGetClient

	return sio
}

func (sio *sqsIO) funcGetClient() *sqs.SQS {
	if sio.client == nil {
		defer recover()

		cfg := aws.NewConfig().
			WithRegion(sio.region).
			WithDisableSSL(sio.disableSSL).
			WithMaxRetries(sio.maxRetries)

		if sio.accessKeyID != "" && sio.secretAccessKey != "" {
			creds := credentials.NewStaticCredentials(sio.accessKeyID, sio.secretAccessKey, sio.sessionToken)
			cfg = cfg.WithCredentials(creds)
		}

		if sio.logLevel > 0 && sio.getLoggerFunc != nil {
			l := sio.getLoggerFunc()
			if l != nil {
				cfg.Logger = l
				cfg.LogLevel = aws.LogLevel(aws.LogLevelType(sio.logLevel))
			}
		}

		sio.client = sqs.New(session.New(), cfg)
	}
	return sio.client
}
