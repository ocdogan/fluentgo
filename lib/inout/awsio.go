package inout

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/lib/config"
	"github.com/ocdogan/fluentgo/lib/log"
)

type awsIO struct {
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
	region          string
	disableSSL      bool
	maxRetries      int
	logLevel        uint
	compressed      bool
	getLoggerFunc   func() log.Logger
}

func newAwsIO(manager InOutManager, config *config.InOutConfig) *awsIO {
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

	awsio := &awsIO{
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		sessionToken:    token,
		region:          region,
		disableSSL:      disableSSL,
		maxRetries:      maxRetries,
		logLevel:        uint(logLevel),
		compressed:      compressed,
	}

	return awsio
}

func (awsio *awsIO) currLogger() log.Logger {
	if awsio.getLoggerFunc != nil {
		return awsio.getLoggerFunc()
	}
	return nil
}

func (awsio *awsIO) getAwsConfig() *aws.Config {
	defer recover()

	cfg := aws.NewConfig().
		WithRegion(awsio.region).
		WithDisableSSL(awsio.disableSSL).
		WithMaxRetries(awsio.maxRetries)

	if awsio.accessKeyID != "" && awsio.secretAccessKey != "" {
		creds := credentials.NewStaticCredentials(awsio.accessKeyID, awsio.secretAccessKey, awsio.sessionToken)
		cfg = cfg.WithCredentials(creds)
	}

	if awsio.logLevel > 0 {
		l := awsio.currLogger()
		if l != nil {
			cfg.Logger = l
			cfg.LogLevel = aws.LogLevel(aws.LogLevelType(awsio.logLevel))
		}
	}

	return cfg
}
