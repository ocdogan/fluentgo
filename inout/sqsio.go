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
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/ocdogan/fluentgo/config"
)

type sqsIO struct {
	awsIO
	queueURL   string
	attributes map[string]*sqs.MessageAttributeValue
	client     *sqs.SQS
	connFunc   func() *sqs.SQS
}

func newSqsIO(manager InOutManager, params map[string]interface{}) *sqsIO {
	awsio := newAwsIO(manager, params)
	if awsio == nil {
		return nil
	}

	var (
		ok            bool
		pName, pValue string
	)

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

	queueURL, ok := config.ParamAsString(params, "queueURL")
	if !ok || queueURL == "" {
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
