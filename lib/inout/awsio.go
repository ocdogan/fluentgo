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
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/ocdogan/fluentgo/lib"
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
	getLoggerFunc   func() log.Logger
}

func newAwsIO(manager InOutManager, params map[string]interface{}) *awsIO {
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

	awsio := &awsIO{
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		sessionToken:    token,
		region:          region,
		disableSSL:      disableSSL,
		maxRetries:      maxRetries,
		logLevel:        uint(logLevel),
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
