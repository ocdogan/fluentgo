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
	"encoding/json"
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
	var (
		s        string
		acl      string
		bucket   *lib.JsonPath
		prefix   *lib.JsonPath
		rootAttr string
		ok       bool
	)

	s, ok = params["bucket"].(string)
	if ok {
		s = strings.TrimSpace(s)
	}
	if s == "" {
		return nil
	}
	bucket = lib.NewJsonPath(s)

	acl, ok = params["acl"].(string)
	if ok {
		acl = strings.TrimSpace(acl)
	}
	if acl == "" {
		acl = s3.BucketCannedACLPublicRead
	}

	s, ok = params["prefix"].(string)
	if ok {
		s = strings.TrimSpace(s)
	}
	prefix = lib.NewJsonPath(s)

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
	return ""
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

func (s3o *s3Out) putMessages(messages []string, bucket, filename string) {
	defer recover()

	client := s3o.getClient()
	if client == nil {
		return
	}

	count := 0
	var buffer *bytes.Buffer

	for _, msg := range messages {
		if msg != "" {
			if count == 0 {
				t := time.Now()
				buffer = bytes.NewBufferString(
					fmt.Sprintf("{\"date\":\"%d.%02d.%02d %02d:%02d:%02d\",\"%s\":[",
						t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), s3o.rootAttr))
			} else {
				buffer.WriteString(",")
			}

			count++
			buffer.WriteString(msg)
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

func (s3o *s3Out) groupMessages(messages []string) map[string]map[string][]string {
	defer recover()

	var buckets map[string]map[string][]string

	if s3o.bucket.IsStatic() && s3o.prefix.IsStatic() {
		bucket, _, err := s3o.bucket.Eval(nil, true)
		if err != nil {
			return nil
		}

		prefix, _, err := s3o.prefix.Eval(nil, true)
		if err != nil {
			return nil
		}
		prefix = strings.TrimSpace(prefix)

		buckets = make(map[string]map[string][]string)

		prefixes := make(map[string][]string)
		prefixes[prefix] = messages

		buckets[bucket] = prefixes
	} else {
		var (
			bucket string
			prefix string
		)

		isBucketStatic := s3o.bucket.IsStatic()
		isPrefixStatic := s3o.prefix.IsStatic()

		if isBucketStatic {
			s, _, err := s3o.bucket.Eval(nil, true)
			if err != nil || len(s) == 0 {
				return nil
			}
			bucket = s
		}

		if isPrefixStatic {
			s, _, err := s3o.prefix.Eval(nil, true)
			if err != nil {
				return nil
			}
			prefix = s
		}

		var (
			ok         bool
			prefixList []string
			bucketMap  map[string][]string
		)

		buckets = make(map[string]map[string][]string)

		for _, msg := range messages {
			if msg != "" {
				var data interface{}

				err := json.Unmarshal([]byte(msg), &data)
				if err != nil {
					continue
				}

				if !isBucketStatic {
					bucket, _, err = s3o.bucket.Eval(data, true)
					if err != nil || len(bucket) == 0 {
						continue
					}
				}

				if !isPrefixStatic {
					prefix, _, err = s3o.prefix.Eval(data, true)
					if err != nil {
						continue
					}
				}

				bucketMap, ok = buckets[bucket]
				if !ok || bucketMap == nil {
					bucketMap = make(map[string][]string)
					buckets[bucket] = bucketMap
				}

				prefixList, _ = bucketMap[prefix]
				bucketMap[prefix] = append(prefixList, msg)
			}
		}
	}
	return buckets
}

func (s3o *s3Out) funcPutMessages(messages []string, filename string) {
	if len(messages) == 0 {
		return
	}

	defer recover()

	buckets := s3o.groupMessages(messages)
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
