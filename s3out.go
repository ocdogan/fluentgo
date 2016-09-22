package main

import (
	"bytes"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3Out struct {
	outHandler
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
	acl             string
	region          string
	bucket          string
	prefix          string
	rootAttr        string
	disableSSL      bool
	compression     bool
	maxRetries      int
	logLevel        uint
	client          *s3.S3
}

var (
	s3oIndex      uint64
	s3oLastMinute int
)

func newS3Out(manager InOutManager, config *inOutConfig) *s3Out {
	if config == nil {
		return nil
	}

	params := config.getParamsMap()

	var (
		accessKeyID     string
		secretAccessKey string
		token           string
		acl             string
		region          string
		bucket          string
		prefix          string
		rootAttr        string
		ok              bool
	)

	bucket, ok = params["bucket"].(string)
	if ok {
		bucket = strings.TrimSpace(bucket)
	}
	if bucket == "" {
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

	var (
		f           float64
		disableSSL  bool
		compression bool
	)

	if disableSSL, ok = params["disableSSL"].(bool); !ok {
		disableSSL = false
	}

	if compression, ok = params["compression"].(bool); !ok {
		compression = false
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
		logLevel = maxInt(int(f), 0)
	}

	oh := newOutHandler(manager, params)
	if oh == nil {
		return nil
	}

	s3o := &s3Out{
		outHandler:      *oh,
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		acl:             acl,
		sessionToken:    token,
		region:          region,
		bucket:          bucket,
		prefix:          prefix,
		disableSSL:      disableSSL,
		compression:     compression,
		maxRetries:      maxRetries,
		rootAttr:        rootAttr,
		logLevel:        uint(logLevel),
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
			if s3o.compression {
				body = compress(body)
			}

			params := &s3.PutObjectInput{
				ACL:           aws.String(s3o.acl),
				Bucket:        aws.String(s3o.bucket),
				Body:          bytes.NewReader(body),
				ContentLength: aws.Int64(int64(len(body))),
			}

			if s3o.compression {
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

		cfg := aws.NewConfig().
			WithRegion(s3o.region).
			WithDisableSSL(s3o.disableSSL).
			WithMaxRetries(s3o.maxRetries)

		if s3o.accessKeyID != "" && s3o.secretAccessKey != "" {
			creds := credentials.NewStaticCredentials(s3o.accessKeyID, s3o.secretAccessKey, s3o.sessionToken)
			cfg = cfg.WithCredentials(creds)
		}

		if s3o.logLevel > 0 {
			l := s3o.GetLogger()
			if l != nil {
				cfg.Logger = l
				cfg.LogLevel = aws.LogLevel(aws.LogLevelType(s3o.logLevel))
			}
		}

		s3o.client = s3.New(session.New(), cfg)
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
