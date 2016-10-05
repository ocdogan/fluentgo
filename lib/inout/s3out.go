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
	"bytes"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/ocdogan/fluentgo/lib"
)

type s3Out struct {
	outHandler
	awsIO
	acl      string
	bucket   string
	prefix   string
	rootAttr string
	client   *s3.S3
}

var (
	s3oIndex      uint64
	s3oLastMinute int
)

func init() {
	RegisterOut("s3", newS3Out)
	RegisterOut("s3out", newS3Out)
}

func newS3Out(manager InOutManager, params map[string]interface{}) OutSender {
	var (
		acl      string
		bucket   string
		prefix   string
		rootAttr string
		ok       bool
	)

	bucket, ok = params["bucket"].(string)
	if ok {
		bucket = strings.TrimSpace(bucket)
	}
	if bucket == "" {
		return nil
	}

	acl, ok = params["acl"].(string)
	if ok {
		acl = strings.TrimSpace(acl)
	}
	if acl == "" {
		acl = s3.BucketCannedACLPublicRead
	}

	prefix, ok = params["prefix"].(string)
	if ok {
		prefix = strings.TrimSpace(prefix)
	}

	rootAttr, ok = params["root"].(string)
	if ok {
		rootAttr = strings.TrimSpace(rootAttr)
	}
	if rootAttr == "" {
		rootAttr = "messages"
	}

	awsio := newAwsIO(manager, params)
	if awsio == nil {
		return nil
	}

	oh := newOutHandler(manager, params)
	if oh == nil {
		return nil
	}

	s3o := &s3Out{
		outHandler: *oh,
		awsIO:      *awsio,
		acl:        acl,
		bucket:     bucket,
		prefix:     prefix,
		rootAttr:   rootAttr,
	}

	s3o.iotype = "S3OUT"

	s3o.runFunc = s3o.funcRunAndWait
	s3o.afterCloseFunc = s3o.funcAfterClose
	s3o.getDestinationFunc = s3o.funcGetObjectName
	s3o.sendChunkFunc = s3o.funcPutMessages

	return s3o
}

func (s3o *s3Out) funcAfterClose() {
	if s3o != nil {
		s3o.client = nil
	}
}

func (s3o *s3Out) funcGetObjectName() string {
	t := time.Now()

	index := uint64(0)
	if s3oLastMinute != t.Minute() {
		index = 0
		s3oLastMinute = t.Minute()

		atomic.StoreUint64(&s3oIndex, 0)
	} else {
		index = atomic.AddUint64(&s3oIndex, 1)
	}

	if s3o.prefix != "" {
		return fmt.Sprintf("%s/%d%02d%02d/%02d%02d%02d%03d", s3o.prefix, t.Year(), t.Month(), t.Day(),
			t.Hour(), t.Minute(), t.Second(), index)
	}
	return fmt.Sprintf("%d%02d%02d/%02d%02d%02d%03d", t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(), index)
}

func (s3o *s3Out) funcPutMessages(messages []string, indexName string) {
	if len(messages) > 0 {
		defer recover()

		client := s3o.getClient()
		if client == nil {
			return
		}

		t := time.Now()
		buffer := bytes.NewBufferString(
			fmt.Sprintf("{\"date\":\"%d.%02d.%02d %02d:%02d:%02d\",\"%s\":[",
				t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), s3o.rootAttr))

		count := 0
		for _, msg := range messages {
			if msg != "" {
				if count > 0 {
					buffer.WriteString(",")
				}
				count++
				buffer.WriteString(msg)
			}
		}
		buffer.WriteString("]}")

		if count > 0 {
			body := buffer.Bytes()
			if s3o.compressed {
				body = lib.Compress(body, s3o.compressType)
			}

			params := &s3.PutObjectInput{
				ACL:           aws.String(s3o.acl),
				Bucket:        aws.String(s3o.bucket),
				Body:          bytes.NewReader(body),
				ContentLength: aws.Int64(int64(len(body))),
			}

			if s3o.compressed {
				params.Key = aws.String(indexName + ".gz")
				params.ContentType = aws.String("application/x-gzip")
			} else {
				params.Key = aws.String(indexName + ".txt")
				params.ContentType = aws.String("text/plain")
			}

			client.PutObject(params)
		}
	}
}

func (s3o *s3Out) getClient() *s3.S3 {
	if s3o.client == nil {
		defer recover()
		s3o.client = s3.New(session.New(), s3o.getAwsConfig())
	}
	return s3o.client
}

func (s3o *s3Out) funcRunAndWait() {
	defer func() {
		recover()
		l := s3o.GetLogger()
		if l != nil {
			l.Println("Stoping 'S3OUT'...")
		}
	}()

	l := s3o.GetLogger()
	if l != nil {
		l.Println("Starting 'S3OUT'...")
	}

	<-s3o.completed
}
