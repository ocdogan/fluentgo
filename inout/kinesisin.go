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
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/ocdogan/fluentgo/config"
	"github.com/ocdogan/fluentgo/lib"
)

type kinesisIn struct {
	kinesisIO
	inHandler
	streamName    string
	shardIterator string
	limit         int64
}

func init() {
	RegisterIn("kinesis", newKinesisIn)
	RegisterIn("kinesisin", newKinesisIn)
}

func newKinesisIn(manager InOutManager, params map[string]interface{}) InProvider {
	ih := newInHandler(manager, params)
	if ih == nil {
		return nil
	}

	kio := newKinesisIO(manager, params)
	if kio == nil {
		return nil
	}

	streamName, ok := config.ParamAsString(params, "streamName")
	if !ok || streamName == "" {
		return nil
	}

	shardIterator, ok := config.ParamAsString(params, "shardIterator")
	if !ok || shardIterator == "" {
		return nil
	}

	limit, _ := config.ParamAsInt64(params, "limit")
	limit = lib.MaxInt64(limit, 1)

	ki := &kinesisIn{
		kinesisIO:     *kio,
		inHandler:     *ih,
		limit:         limit,
		shardIterator: shardIterator,
	}

	ki.iotype = "KINESISIN"

	ki.runFunc = ki.funcReceive

	return ki
}

func (ki *kinesisIn) funcReceive() {
	defer ki.InformStop()
	ki.InformStart()

	completed := false

	maxMessageSize := ki.getMaxMessageSize()

	params := &kinesis.GetRecordsInput{
		ShardIterator: aws.String(ki.shardIterator), // Required
		Limit:         aws.Int64(ki.limit),
	}

	loop := 0
	l := ki.GetLogger()

	for !completed {
		select {
		case <-ki.completed:
			completed = true
			ki.Close()
			return
		default:
			if completed {
				return
			}

			ki.Connect()

			client := ki.client
			if client == nil {
				completed = true
				return
			}

			resp, err := client.GetRecords(params)
			if err != nil {
				if l != nil {
					l.Println(err)
				}
				time.Sleep(100 * time.Microsecond)
				continue
			}

			if resp == nil {
				time.Sleep(100 * time.Microsecond)
				continue
			}

			params.ShardIterator = resp.NextShardIterator
			if params.ShardIterator == nil {
				time.Sleep(100 * time.Microsecond)
				continue
			}

			if len(resp.Records) == 0 {
				time.Sleep(100 * time.Microsecond)
				continue
			}

			var wg sync.WaitGroup
			for _, r := range resp.Records {
				wg.Add(1)
				go func(kin *kinesisIn, rec *kinesis.Record, wg *sync.WaitGroup) {
					defer wg.Done()

					if err == nil {
						kin.queueMessage(rec.Data, maxMessageSize)
					} else {
						l := ki.GetLogger()
						if l != nil {
							l.Println(err)
						}
					}
				}(ki, r, &wg)
			}

			wg.Wait()

			loop++
			if loop%100 == 0 {
				loop = 0
				time.Sleep(time.Millisecond)
			}
		}
	}
}

func (ki *kinesisIn) Connect() {
	if ki.client == nil && ki.connFunc == nil {
		ki.connFunc()
	}
}
