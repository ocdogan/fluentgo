package inout

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/lib/config"
	"github.com/ocdogan/fluentgo/lib/log"
)

type kinesisIO struct {
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
	region          string
	streamName      string
	shardIterator   string
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

	params := config.GetParamsMap()

	var (
		ok              bool
		accessKeyID     string
		secretAccessKey string
		token           string
		region          string
	)

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
		compressed bool
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

	if compressed, ok = params["compressed"].(bool); !ok {
		compressed = false
	}

	kio := &kinesisIO{
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		sessionToken:    token,
		region:          region,
		disableSSL:      disableSSL,
		maxRetries:      maxRetries,
		logLevel:        uint(logLevel),
		compressed:      compressed,
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
