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
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/ocdogan/fluentgo/config"
	"github.com/ocdogan/fluentgo/lib"
)

type s3Out struct {
	outHandler
	awsIO
	acl      string
	rootAttr string
	bucket   *lib.JsonPath
	prefix   *lib.JsonPath
	client   *s3.S3
}

var (
	s3oIndex         uint64
	s3oLastIndexDate = time.Now()
)

func init() {
	RegisterOut("s3", newS3Out)
	RegisterOut("s3out", newS3Out)
}

func newS3Out(manager InOutManager, params map[string]interface{}) OutSender {
	bck, ok := config.ParamAsString(params, "bucket")
	if !ok && bck == "" {
		return nil
	}
	bucket := lib.NewJsonPath(bck)

	acl, ok := config.ParamAsString(params, "acl")
	if !ok || acl == "" {
		acl = s3.BucketCannedACLPublicRead
	}

	prfx, _ := config.ParamAsString(params, "prefix")
	prefix := lib.NewJsonPath(prfx)

	rootAttr, ok := config.ParamAsString(params, "root")
	if !ok || rootAttr == "" {
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

	s3o.runFunc = s3o.waitComplete
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
	return "null"
}

func (s3o *s3Out) getFilenameIndex() uint64 {
	t := time.Now()

	index := uint64(0)
	d := t.Sub(s3oLastIndexDate)

	if d.Hours() > 0 || d.Minutes() > 0 {
		s3oLastIndexDate = t

		index = 0
		atomic.StoreUint64(&s3oIndex, 0)
	} else {
		index = atomic.AddUint64(&s3oIndex, 1)
	}
	return index
}

func (s3o *s3Out) getFilename(prefix string) string {
	t := time.Now()
	index := s3o.getFilenameIndex()

	if prefix != "" {
		return fmt.Sprintf("%s/%d%02d%02d/%02d%02d%02d%03d", prefix, t.Year(), t.Month(), t.Day(),
			t.Hour(), t.Minute(), t.Second(), index)
	}
	return fmt.Sprintf("%d%02d%02d/%02d%02d%02d%03d", t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(), index)
}

func (s3o *s3Out) putMessages(messages []ByteArray, bucket, filename string) {
	defer recover()

	client := s3o.getClient()
	if client == nil {
		return
	}

	count := 0
	var buffer *bytes.Buffer

	for _, msg := range messages {
		if len(msg) > 0 {
			if count == 0 {
				t := time.Now()
				buffer = bytes.NewBufferString(
					fmt.Sprintf("{\"date\":\"%d.%02d.%02d %02d:%02d:%02d\",\"%s\":[",
						t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), s3o.rootAttr))
			} else {
				buffer.WriteString(",")
			}

			count++
			buffer.Write([]byte(msg))
		}
	}

	if count > 0 {
		buffer.WriteString("]}")

		body := buffer.Bytes()
		if s3o.compressed {
			body = lib.Compress(body, s3o.compressType)
		}

		params := &s3.PutObjectInput{
			ACL:           aws.String(s3o.acl),
			Bucket:        aws.String(bucket),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(int64(len(body))),
		}

		if s3o.compressed {
			params.Key = aws.String(filename + ".gz")
			params.ContentType = aws.String("application/x-gzip")
		} else {
			params.Key = aws.String(filename + ".txt")
			params.ContentType = aws.String("text/plain")
		}

		client.PutObject(params)
	}
}

func (s3o *s3Out) funcPutMessages(messages []ByteArray, filename string) {
	if len(messages) == 0 {
		return
	}
	defer recover()

	buckets := s3o.groupMessages(messages, s3o.bucket, s3o.prefix)
	if buckets == nil {
		return
	}

	for bucket, bucketMap := range buckets {
		for prefix, msgs := range bucketMap {
			filename := s3o.getFilename(prefix)
			s3o.putMessages(msgs, bucket, filename)
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
