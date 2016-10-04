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

	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/lib/log"
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

	pServers, ok := params["servers"].(string)
	if ok {
		pServers = strings.TrimSpace(pServers)
		if pServers != "" {
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
	}
	if len(servers) == 0 {
		return nil
	}

	topic, ok := params["topic"].(string)
	if ok {
		topic = strings.TrimSpace(topic)
	}
	if topic == "" {
		return nil
	}

	partition := int32(0)
	if f, ok := params["partition"].(float64); ok {
		partition = lib.MinInt32(100000, lib.MaxInt32(0, int32(f)))
	}

	offset := kafka.StartOffsetNewest
	if s, ok := params["offset"].(string); ok {
		s = strings.TrimSpace(s)
		if s != "" {
			s = strings.ToLower(s)
			if s == "oldest" {
				offset = kafka.StartOffsetOldest
			}
		}
	}

	brokerConf := kafka.NewBrokerConf(id.String())

	if f, ok := params["dialTimeoutMSec"].(float64); ok {
		brokerConf.DialTimeout = time.Duration(lib.MinInt(60000, lib.MaxInt(500, int(f)))) * time.Millisecond
	}

	if f, ok := params["dialRetryWaitMSec"].(float64); ok {
		brokerConf.DialRetryWait = time.Duration(lib.MinInt(15000, lib.MaxInt(10, int(f)))) * time.Millisecond
	}

	if f, ok := params["leaderRetryWaitMSec"].(float64); ok {
		brokerConf.LeaderRetryWait = time.Duration(lib.MinInt(15000, lib.MaxInt(10, int(f)))) * time.Millisecond
	}

	if f, ok := params["retryErrWaitMSec"].(float64); ok {
		brokerConf.RetryErrWait = time.Duration(lib.MinInt(15000, lib.MaxInt(10, int(f)))) * time.Millisecond
	}

	if f, ok := params["retryErrLimit"].(float64); ok {
		brokerConf.RetryErrLimit = lib.MinInt(20, lib.MaxInt(1, int(f)))
	}

	if f, ok := params["readTimeout"].(float64); ok {
		brokerConf.ReadTimeout = time.Duration(lib.MinInt(60000, lib.MaxInt(1000, int(f)))) * time.Millisecond
	}

	if f, ok := params["dialRetryLimit"].(float64); ok {
		brokerConf.DialRetryLimit = lib.MinInt(20, lib.MaxInt(1, int(f)))
	}

	if f, ok := params["leaderRetryLimit"].(float64); ok {
		brokerConf.LeaderRetryLimit = lib.MinInt(20, lib.MaxInt(1, int(f)))
	}

	if f, ok := params["retryErrLimit"].(float64); ok {
		brokerConf.RetryErrLimit = lib.MinInt(20, lib.MaxInt(1, int(f)))
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
