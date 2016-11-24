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

	dialTimeout, ok := config.ParamAsDuration(params, "dialTimeoutMSec")
	if ok {
		brokerConf.DialTimeout = lib.MinDuration(60000, lib.MaxDuration(500, dialTimeout)) * time.Millisecond
	}

	dialRetryWait, ok := config.ParamAsDuration(params, "dialRetryWaitMSec")
	if ok {
		brokerConf.DialRetryWait = lib.MinDuration(15000, lib.MaxDuration(10, dialRetryWait)) * time.Millisecond
	}

	dialRetryLimit, ok := config.ParamAsInt(params, "dialRetryLimit")
	if ok {
		brokerConf.DialRetryLimit = lib.MinInt(20, lib.MaxInt(1, dialRetryLimit))
	}

	leaderRetryWait, ok := config.ParamAsDuration(params, "leaderRetryWaitMSec")
	if ok {
		brokerConf.LeaderRetryWait = lib.MinDuration(15000, lib.MaxDuration(10, leaderRetryWait)) * time.Millisecond
	}

	leaderRetryLimit, ok := config.ParamAsInt(params, "leaderRetryLimit")
	if ok {
		brokerConf.LeaderRetryLimit = lib.MinInt(20, lib.MaxInt(1, leaderRetryLimit))
	}

	retryErrWait, ok := config.ParamAsDuration(params, "retryErrWaitMSec")
	if ok {
		brokerConf.RetryErrWait = lib.MinDuration(15000, lib.MaxDuration(10, retryErrWait)) * time.Millisecond
	}

	retryErrLimit, ok := config.ParamAsInt(params, "retryErrLimit")
	if ok {
		brokerConf.RetryErrLimit = lib.MinInt(20, lib.MaxInt(1, retryErrLimit))
	}

	readTimeout, ok := config.ParamAsDuration(params, "readTimeout")
	if ok {
		brokerConf.ReadTimeout = lib.MinDuration(60000, lib.MaxDuration(1000, readTimeout)) * time.Millisecond
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
				kio.lg.Printf("Failed to create KAFKAIN consumer: %s\n", err)
			}
			return err
		}

		kio.broker = broker
	}
	return nil
}
