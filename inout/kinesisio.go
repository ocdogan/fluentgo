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
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type kinesisIO struct {
	awsIO
	client   *kinesis.Kinesis
	connFunc func() *kinesis.Kinesis
}

func newKinesisIO(manager InOutManager, params map[string]interface{}) *kinesisIO {
	awsio := newAwsIO(manager, params)
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
		kio.client = kinesis.New(session.New(), kio.getAwsConfig())
	}
	return kio.client
}
