package inout

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/ocdogan/fluentgo/lib/config"
	"github.com/ocdogan/fluentgo/lib/log"
)

type kinesisIO struct {
	awsIO
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
	region          string
	disableSSL      bool
	maxRetries      int
	logLevel        uint
	compressed      bool
	client          *kinesis.Kinesis
	connFunc        func() *kinesis.Kinesis
	getLoggerFunc   func() log.Logger
}

func newKinesisIO(manager InOutManager, config *config.InOutConfig) *kinesisIO {
	if config == nil {
		return nil
	}

	awsio := newAwsIO(manager, config)
	if awsio == nil {
		return nil
	}

	kio := &kinesisIO{
		awsIO: *awsio,
	}

	kio.connFunc = kio.funcGetClient

	return kio
}

func (kio *kinesisIO) funcGetClient() *kinesis.Kinesis {
	if kio.client == nil {
		defer recover()

		cfg := aws.NewConfig().
			WithRegion(kio.region).
			WithDisableSSL(kio.disableSSL).
			WithMaxRetries(kio.maxRetries)

		if kio.accessKeyID != "" && kio.secretAccessKey != "" {
			creds := credentials.NewStaticCredentials(kio.accessKeyID, kio.secretAccessKey, kio.sessionToken)
			cfg = cfg.WithCredentials(creds)
		}

		if kio.logLevel > 0 && kio.getLoggerFunc != nil {
			l := kio.getLoggerFunc()
			if l != nil {
				cfg.Logger = l
				cfg.LogLevel = aws.LogLevel(aws.LogLevelType(kio.logLevel))
			}
		}

		kio.client = kinesis.New(session.New(), cfg)
	}
	return kio.client
}
