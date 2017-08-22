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

	"github.com/ocdogan/fluentgo/config"
	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/log"
	"gopkg.in/olivere/elastic.v2"
)

type elasticOut struct {
	outHandler
	indexPrefix *lib.JsonPath
	indexType   *lib.JsonPath
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

	url, ok := config.ParamAsString(params, "url")
	if !ok || url == "" {
		return nil
	}

	var opts []elastic.ClientOptionFunc
	opts = append(opts, elastic.SetURL(url))

	user, ok := config.ParamAsString(params, "userName")
	if ok && user != "" {
		pwd, ok := config.ParamAsString(params, "password")
		if ok && pwd != "" {
			opts = append(opts, elastic.SetBasicAuth(user, pwd))
		}
	}

	compression, ok := config.ParamAsBool(params, "compression")
	if !ok {
		compression = false
	}
	opts = append(opts, elastic.SetGzip(compression))

	prefix, ok := config.ParamAsString(params, "index.prefix")
	if !ok || prefix == "" {
		prefix = "logstash-"
	} else {
		prefix = strings.ToLower(prefix)
		if prefix[len(prefix)-1] != '-' {
			prefix += "-"
		}
	}
	indexPrefix := lib.NewJsonPath(prefix)

	idxtype, ok := config.ParamAsString(params, "index.type")
	if !ok || idxtype == "" {
		idxtype = "elasticout"
	} else {
		idxtype = strings.ToLower(idxtype)
	}
	indexType := lib.NewJsonPath(idxtype)

	maxRetries, ok := config.ParamAsIntWithLimit(params, "maxRetries", 1, 50)
	opts = append(opts, elastic.SetMaxRetries(maxRetries))

	var lg log.Logger

	lg = newLoggerForParams(params, "error")
	opts = append(opts, elastic.SetErrorLog(lg))

	lg = newLoggerForParams(params, "info")
	opts = append(opts, elastic.SetInfoLog(lg))

	lg = newLoggerForParams(params, "trace")
	opts = append(opts, elastic.SetTraceLog(lg))

	hcenabled, ok := config.ParamAsBool(params, "healthcheck.enabled")
	opts = append(opts, elastic.SetHealthcheck(ok && hcenabled))

	if ok && hcenabled {
		hctimeout, _ := config.ParamAsDurationWithLimit(params, "healthcheck.timeout", 1, 60)
		hctimeout *= time.Second
		opts = append(opts, elastic.SetHealthcheckTimeout(hctimeout))

		hcinterval, _ := config.ParamAsDurationWithLimit(params, "healthcheck.interval", 5, 300)
		hcinterval *= time.Second
		opts = append(opts, elastic.SetHealthcheckInterval(hcinterval))

		hctimeoutStartup, _ := config.ParamAsDurationWithLimit(params, "healthcheck.timeoutStartup", 5, 300)
		hctimeoutStartup *= time.Second
		opts = append(opts, elastic.SetHealthcheckTimeoutStartup(hctimeoutStartup))
	}

	snenabled, ok := config.ParamAsBool(params, "sniffing.enabled")
	opts = append(opts, elastic.SetSniff(ok && snenabled))

	if ok && snenabled {
		sntimeout, _ := config.ParamAsDurationWithLimit(params, "sniffing.timeout", 1, 60)
		sntimeout *= time.Second
		opts = append(opts, elastic.SetSnifferTimeout(sntimeout))

		sninterval, _ := config.ParamAsDurationWithLimit(params, "sniffing.interval", 5, 300)
		sninterval *= time.Second
		opts = append(opts, elastic.SetSnifferInterval(sninterval))

		sntimeoutStartup, _ := config.ParamAsDurationWithLimit(params, "sniffing.timeoutStartup", 5, 300)
		sntimeoutStartup *= time.Second
		opts = append(opts, elastic.SetSnifferTimeoutStartup(sntimeoutStartup))
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

	eo.runFunc = eo.waitComplete
	eo.afterCloseFunc = eo.funcAfterClose
	eo.getDestinationFunc = eo.funcDestination
	eo.sendChunkFunc = eo.funcPutMessages

	return eo
}

func newLoggerForParams(params map[string]interface{}, paramType string) log.Logger {
	if params == nil {
		return nil
	}

	paramType = fmt.Sprintf("logging.%s.", paramType)

	enabled, ok := config.ParamAsBool(params, paramType+"enabled")
	if !ok || !enabled {
		return nil
	}

	lc := &config.LogConfig{
		Enabled: true,
	}

	lc.Path, ok = config.ParamAsString(params, paramType+"path")
	lc.Type, ok = config.ParamAsString(params, paramType+"type")

	rollingSize, ok := config.ParamAsInt(params, paramType+"rollingSize")
	if ok && rollingSize > 0 {
		lc.RollingSize = rollingSize
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

func (eo *elasticOut) funcDestination() string {
	return "null"
}

func (eo *elasticOut) getIndexName(prefix string) string {
	t := time.Now()
	if prefix != "" {
		return fmt.Sprintf("%s%d.%02d.%02d", prefix, t.Year(), t.Month(), t.Day())
	}
	return fmt.Sprintf("%d.%02d.%02d", t.Year(), t.Month(), t.Day())
}

func (eo *elasticOut) putMessages(messages []ByteArray, indexName, indexType string) {
	if len(messages) == 0 {
		return
	}
	defer recover()

	doSend := false
	bulkRequest := eo.client.Bulk()

	for _, msg := range messages {
		if len(msg) > 0 {
			doSend = true

			req := elastic.NewBulkIndexRequest().Index(indexName).Type(indexType).Doc(string(msg))
			bulkRequest = bulkRequest.Add(req)
		}
	}

	if doSend {
		bulkRequest.Do()
	}
}

func (eo *elasticOut) funcPutMessages(messages []ByteArray, filename string) {
	if len(messages) == 0 {
		return
	}
	defer recover()

	indexPrefixes := eo.groupMessages(messages, eo.indexPrefix, eo.indexType)
	if indexPrefixes == nil {
		return
	}

	for indexPrefix, indexPrefixMap := range indexPrefixes {
		indexName := eo.getIndexName(indexPrefix)

		for indexType, msgs := range indexPrefixMap {
			eo.putMessages(msgs, indexName, indexType)
		}
	}
}
