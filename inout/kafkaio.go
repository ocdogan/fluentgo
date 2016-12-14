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
	"time"

	"github.com/ocdogan/fluentgo/config"
	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/log"
	"github.com/optiopay/kafka"
)

type kafkaIO struct {
	partition  int32
	offset     int
	topic      string
	servers    []string
	lg         log.Logger
	broker     *kafka.Broker
	brokerConf *kafka.BrokerConf
}

func newKafkaIO(manager InOutManager, id lib.UUID, params map[string]interface{}) *kafkaIO {
	var servers []string

	pServers, ok := config.ParamAsString(params, "servers")
	if ok && pServers != "" {
		aServers := strings.Split(pServers, ";")
		if len(aServers) > 0 {
			for _, s := range aServers {
				s = strings.TrimSpace(s)
				if s != "" {
					servers = append(servers, s)
				}
			}
		}
	}
	if len(servers) == 0 {
		return nil
	}

	topic, ok := config.ParamAsString(params, "topic")
	if !ok || topic == "" {
		return nil
	}

	partition, ok := config.ParamAsInt32(params, "partition")
	partition = lib.MinInt32(100000, lib.MaxInt32(0, partition))

	offset := kafka.StartOffsetNewest

	s, ok := config.ParamAsString(params, "offset")
	if ok && s != "" {
		s = strings.ToLower(s)
		if s == "oldest" {
			offset = kafka.StartOffsetOldest
		}
	}

	brokerConf := kafka.NewBrokerConf(id.String())

	allowTopicCreation, ok := config.ParamAsBool(params, "allowTopicCreation")
	if ok {
		brokerConf.AllowTopicCreation = allowTopicCreation
	}

	dialTimeout, ok := config.ParamAsDurationWithLimit(params, "dialTimeoutMSec", 500, 60000)
	if ok {
		brokerConf.DialTimeout = dialTimeout * time.Millisecond
	}

	dialRetryWait, ok := config.ParamAsDurationWithLimit(params, "dialRetryWaitMSec", 10, 15000)
	if ok {
		brokerConf.DialRetryWait = dialRetryWait * time.Millisecond
	}

	dialRetryLimit, ok := config.ParamAsIntWithLimit(params, "dialRetryLimit", 1, 20)
	if ok {
		brokerConf.DialRetryLimit = dialRetryLimit
	}

	leaderRetryWait, ok := config.ParamAsDurationWithLimit(params, "leaderRetryWaitMSec", 10, 15000)
	if ok {
		brokerConf.LeaderRetryWait = leaderRetryWait * time.Millisecond
	}

	leaderRetryLimit, ok := config.ParamAsIntWithLimit(params, "leaderRetryLimit", 1, 20)
	if ok {
		brokerConf.LeaderRetryLimit = leaderRetryLimit
	}

	retryErrWait, ok := config.ParamAsDurationWithLimit(params, "retryErrWaitMSec", 10, 15000)
	if ok {
		brokerConf.RetryErrWait = retryErrWait * time.Millisecond
	}

	retryErrLimit, ok := config.ParamAsIntWithLimit(params, "retryErrLimit", 1, 20)
	if ok {
		brokerConf.RetryErrLimit = retryErrLimit
	}

	readTimeout, ok := config.ParamAsDurationWithLimit(params, "readTimeout", 1000, 60000)
	if ok {
		brokerConf.ReadTimeout = readTimeout * time.Millisecond
	}

	kio := &kafkaIO{
		partition:  partition,
		offset:     offset,
		servers:    servers,
		topic:      topic,
		brokerConf: &brokerConf,
		lg:         manager.GetLogger(),
	}

	return kio
}

func (kio *kafkaIO) dial() error {
	if kio.broker == nil {
		broker, err := kafka.Dial(kio.servers, *kio.brokerConf)
		if err != nil {
			if kio.lg != nil {
				kio.lg.Printf("Failed to create KAFKA connection: %s\n", err)
			}
			return err
		}

		kio.broker = broker
	}
	return nil
}
