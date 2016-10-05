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
	"fmt"
	"strings"
	"time"

	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/lib/config"
	"github.com/ocdogan/fluentgo/lib/log"
	"gopkg.in/olivere/elastic.v2"
)

type elasticOut struct {
	outHandler
	indexPrefix string
	indexType   string
	client      *elastic.Client
}

func init() {
	RegisterOut("elastic", newElasticOut)
	RegisterOut("elasticsearch", newElasticOut)
}

func newElasticOut(manager InOutManager, params map[string]interface{}) OutSender {
	oh := newOutHandler(manager, params)
	if oh == nil {
		return nil
	}

	var (
		s  string
		ok bool
	)

	s, ok = params["url"].(string)
	if !ok {
		return nil
	}
	url := strings.TrimSpace(s)
	if url == "" {
		return nil
	}

	var opts []elastic.ClientOptionFunc
	opts = append(opts, elastic.SetURL(url))

	s, ok = params["userName"].(string)
	if ok {
		user := strings.TrimSpace(s)
		if user != "" {
			s, ok = params["password"].(string)
			if ok {
				pwd := strings.TrimSpace(s)
				if pwd != "" {
					opts = append(opts, elastic.SetBasicAuth(user, pwd))
				}
			}
		}
	}

	var (
		b  bool
		i  int
		f  float64
		d  time.Duration
		lg log.Logger
	)

	if b, ok = params["compression"].(bool); !ok {
		b = false
	}
	opts = append(opts, elastic.SetGzip(b))

	s, ok = params["index.prefix"].(string)
	if !ok {
		s = "logstash-"
	} else {
		s = strings.TrimSpace(s)
		if s == "" {
			s = "logstash-"
		} else {
			s = strings.ToLower(s)
			if s[len(s)-1] != '-' {
				s += "-"
			}
		}
	}
	indexPrefix := s

	s, ok = params["index.type"].(string)
	if !ok {
		s = "elasticout"
	} else {
		s = strings.TrimSpace(s)
		if s == "" {
			s = "elasticout"
		} else {
			s = strings.ToLower(s)
		}
	}
	indexType := s

	i = 0
	if f, ok = params["maxRetries"].(float64); ok {
		i = int(f)
	}
	if !ok || i < 1 {
		i = 1
	}
	opts = append(opts, elastic.SetMaxRetries(lib.MinInt(50, i)))

	lg = newLoggerForParams(params, "error")
	opts = append(opts, elastic.SetErrorLog(lg))

	lg = newLoggerForParams(params, "info")
	opts = append(opts, elastic.SetInfoLog(lg))

	lg = newLoggerForParams(params, "trace")
	opts = append(opts, elastic.SetTraceLog(lg))

	b, ok = params["healthcheck.enabled"].(bool)
	opts = append(opts, elastic.SetHealthcheck(ok && b))

	if ok && b {
		i = 0
		if f, ok = params["healthcheck.timeout"].(float64); ok {
			i = int(f)
		}
		d = time.Duration(lib.MinInt(60, lib.MaxInt(1, i))) * time.Second
		opts = append(opts, elastic.SetHealthcheckTimeout(d))

		i = 0
		if f, ok = params["healthcheck.interval"].(float64); ok {
			i = int(f)
		}
		d = time.Duration(lib.MinInt(300, lib.MaxInt(5, i))) * time.Second
		opts = append(opts, elastic.SetHealthcheckInterval(d))

		i = 0
		if f, ok = params["healthcheck.timeoutStartup"].(float64); ok {
			i = int(f)
		}
		d = time.Duration(lib.MinInt(300, lib.MaxInt(5, i))) * time.Second
		opts = append(opts, elastic.SetHealthcheckTimeoutStartup(d))
	}

	b, ok = params["sniffing.enabled"].(bool)
	opts = append(opts, elastic.SetSniff(ok && b))

	if ok && b {
		i = 0
		if f, ok = params["sniffing.timeout"].(float64); ok {
			i = int(f)
		}
		d = time.Duration(lib.MinInt(60, lib.MaxInt(1, i))) * time.Second
		opts = append(opts, elastic.SetSnifferTimeout(d))

		i = 0
		if f, ok = params["sniffing.interval"].(float64); ok {
			i = int(f)
		}
		d = time.Duration(lib.MinInt(300, lib.MaxInt(5, i))) * time.Second
		opts = append(opts, elastic.SetSnifferInterval(d))

		i = 0
		if f, ok = params["sniffing.timeoutStartup"].(float64); ok {
			i = int(f)
		}
		d = time.Duration(lib.MinInt(300, lib.MaxInt(5, i))) * time.Second
		opts = append(opts, elastic.SetSnifferTimeoutStartup(d))
	}

	client, err := elastic.NewClient(opts...)
	if err != nil || client == nil {
		return nil
	}

	eo := &elasticOut{
		outHandler:  *oh,
		client:      client,
		indexType:   indexType,
		indexPrefix: indexPrefix,
	}

	eo.iotype = "ELASTICOUT"

	eo.runFunc = eo.funcWait
	eo.afterCloseFunc = eo.funcAfterClose
	eo.getDestinationFunc = eo.funcGetIndexName
	eo.sendChunkFunc = eo.funcSendMessagesChunk

	return eo
}

func newLoggerForParams(params map[string]interface{}, paramType string) log.Logger {
	if params == nil {
		return nil
	}

	paramType = fmt.Sprintf("logging.%s.", paramType)

	var (
		i     int
		f     float64
		s     string
		b, ok bool
	)

	b, ok = params[paramType+"enabled"].(bool)
	if !ok || !b {
		return nil
	}

	lc := &config.LogConfig{
		Enabled: true,
	}

	s, ok = params[paramType+"path"].(string)
	if ok {
		lc.Path = strings.TrimSpace(s)
	}

	s, ok = params[paramType+"type"].(string)
	if ok {
		lc.Type = strings.TrimSpace(s)
	}

	i = 0
	if f, ok = params[paramType+"rollingSize"].(float64); ok {
		i = int(f)
	}
	if i > 0 {
		lc.RollingSize = i
	}

	return log.NewLogger(lc)
}

func (eo *elasticOut) funcAfterClose() {
	if eo != nil {
		client := eo.client
		if client != nil {
			eo.client = nil
			client.Stop()
		}
	}
}

func (eo *elasticOut) funcGetIndexName() string {
	if eo.indexPrefix != "" {
		t := time.Now()
		return fmt.Sprintf("%s%d.%02d.%02d", eo.indexPrefix, t.Year(), t.Month(), t.Day())
	}
	return ""
}

func (eo *elasticOut) funcSendMessagesChunk(messages []string, indexName string) {
	if len(messages) > 0 {
		defer recover()

		doSend := false
		bulkRequest := eo.client.Bulk()

		for _, msg := range messages {
			if msg != "" {
				doSend = true

				req := elastic.NewBulkIndexRequest().Index(indexName).Type(eo.indexType).Doc(msg)
				bulkRequest = bulkRequest.Add(req)
			}
		}

		if doSend {
			bulkRequest.Do()
		}
	}
}

func (eo *elasticOut) funcWait() {
	defer func() {
		recover()
		l := eo.GetLogger()
		if l != nil {
			l.Println("Stoping 'ELASTICOUT'...")
		}
	}()

	l := eo.GetLogger()
	if l != nil {
		l.Println("Starting 'ELASTICOUT'...")
	}

	<-eo.completed
}
