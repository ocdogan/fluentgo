package main

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/olivere/elastic.v2"
)

type elasticOut struct {
	outHandler
	indexPrefix string
	indexType   string
	client      *elastic.Client
}

func newElasticOut(manager InOutManager, config *inOutConfig) *elasticOut {
	if config == nil {
		return nil
	}

	params := make(map[string]interface{}, len(config.Params))
	for _, p := range config.Params {
		params[p.Name] = p.Value
	}

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
		lg Logger
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
			if s[len(s)-1] != "-"[0] {
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
	opts = append(opts, elastic.SetMaxRetries(minInt(50, i)))

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
		d = time.Duration(minInt(60, maxInt(1, i))) * time.Second
		opts = append(opts, elastic.SetHealthcheckTimeout(d))

		i = 0
		if f, ok = params["healthcheck.interval"].(float64); ok {
			i = int(f)
		}
		d = time.Duration(minInt(300, maxInt(5, i))) * time.Second
		opts = append(opts, elastic.SetHealthcheckInterval(d))

		i = 0
		if f, ok = params["healthcheck.timeoutStartup"].(float64); ok {
			i = int(f)
		}
		d = time.Duration(minInt(300, maxInt(5, i))) * time.Second
		opts = append(opts, elastic.SetHealthcheckTimeoutStartup(d))
	}

	b, ok = params["sniffing.enabled"].(bool)
	opts = append(opts, elastic.SetSniff(ok && b))

	if ok && b {
		i = 0
		if f, ok = params["sniffing.timeout"].(float64); ok {
			i = int(f)
		}
		d = time.Duration(minInt(60, maxInt(1, i))) * time.Second
		opts = append(opts, elastic.SetSnifferTimeout(d))

		i = 0
		if f, ok = params["sniffing.interval"].(float64); ok {
			i = int(f)
		}
		d = time.Duration(minInt(300, maxInt(5, i))) * time.Second
		opts = append(opts, elastic.SetSnifferInterval(d))

		i = 0
		if f, ok = params["sniffing.timeoutStartup"].(float64); ok {
			i = int(f)
		}
		d = time.Duration(minInt(300, maxInt(5, i))) * time.Second
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

	eo.runFunc = eo.funcWait
	eo.afterCloseFunc = eo.funcAfterClose
	eo.getDestinationFunc = eo.funcGetIndexName
	eo.sendChunkFunc = eo.funcSendMessagesChunk

	return eo
}

func newLoggerForParams(params map[string]interface{}, paramType string) Logger {
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

	lc := &logConfig{
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

	return newLogger(lc)
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
		defer func() {
			recover()
			if eo.manager != nil {
				eo.manager.DoSleep()
			}
		}()

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
