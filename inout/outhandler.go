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
	"encoding/json"

	"github.com/ocdogan/fluentgo/lib"
)

type outHandler struct {
	ioHandler
	chunkLength        int
	concurrency        int
	getDestinationFunc func() string
	canSendFunc        func(messages []string) bool
	sendChunkFunc      func(messages []string, destination string)
}

func newOutHandler(manager InOutManager, params map[string]interface{}) *outHandler {
	ioh := newIOHandler(manager, params)
	if ioh == nil {
		return nil
	}

	chunkLength := 1
	concurrency := 1

	if params != nil {
		var (
			f  float64
			ok bool
		)

		if f, ok = params["chunkLength"].(float64); ok {
			chunkLength = int(f)
		}
		if !ok || chunkLength < 1 {
			chunkLength = 1
		}

		if f, ok = params["concurrency"].(float64); ok {
			concurrency = int(f)
		}
		if !ok || concurrency < 0 {
			concurrency = 0
		}
	}

	return &outHandler{
		ioHandler:   *ioh,
		chunkLength: chunkLength,
		concurrency: lib.MinInt(20, lib.MaxInt(1, concurrency)),
	}
}

func (o *outHandler) GetDestination() string {
	if o.getDestinationFunc != nil {
		return o.getDestinationFunc()
	}
	return ""
}

func (o *outHandler) CanSend(messages []string) bool {
	if o.canSendFunc != nil {
		return len(messages) > 0 && o.canSendFunc(messages)
	}
	return len(messages) > 0
}

func (o *outHandler) sendChunkAsync(messages []string, destination string, wg *lib.WorkGroup) {
	defer wg.Done()
	if o.sendChunkFunc != nil {
		o.sendChunkFunc(messages, destination)
	}
}

func (o *outHandler) sendChunk(messages []string, destination string) {
	if o.sendChunkFunc != nil {
		o.sendChunkFunc(messages, destination)
	}
}

func (o *outHandler) Send(messages []string) {
	if !o.CanSend(messages) {
		return
	}

	defer recover()

	mlen := len(messages)
	if mlen > 0 {
		chunkCount := mlen / o.chunkLength
		if mlen%o.chunkLength > 0 {
			chunkCount++
		}

		var (
			chunkLen, chunkStart, chunkEnd int
		)

		destination := o.GetDestination()
		if o.concurrency > 1 && chunkCount > 1 {
			wg := lib.WorkGroup{}
			lastIndex := chunkCount - 1

			for i := 0; i < chunkCount; i++ {
				if !o.Processing() {
					return
				}

				chunkStart = i * o.chunkLength
				chunkEnd = lib.MinInt(chunkStart+o.chunkLength, mlen)

				chunkLen = chunkEnd - chunkStart
				if chunkLen > 0 {
					chunk := make([]string, chunkLen)
					copy(chunk, messages[chunkStart:chunkEnd])

					wg.Add(1)
					go o.sendChunkAsync(chunk, destination, &wg)
				}

				if i == lastIndex || wg.Count() == o.concurrency {
					wg.Wait()
				}
			}

			wg.Wait()
		} else {
			for i := 0; i < chunkCount; i++ {
				if !o.Processing() {
					return
				}

				chunkStart = i * o.chunkLength
				chunkEnd = lib.MinInt(chunkStart+o.chunkLength, mlen)

				chunkLen = chunkEnd - chunkStart
				if chunkLen > 0 {
					chunk := make([]string, chunkLen)
					copy(chunk, messages[chunkStart:chunkEnd])

					o.sendChunk(chunk, destination)
				}
			}
		}
	}
}

func (oh *outHandler) groupMessages(messages []string, primaryPath, secondaryPath *lib.JsonPath) map[string]map[string][]string {
	defer recover()

	var primaries map[string]map[string][]string

	if primaryPath.IsStatic() && secondaryPath.IsStatic() {
		primary, _, err := primaryPath.Eval(nil, true)
		if err != nil {
			return nil
		}

		secondary, _, err := secondaryPath.Eval(nil, true)
		if err != nil {
			return nil
		}

		primaries = make(map[string]map[string][]string)

		secondaries := make(map[string][]string)
		secondaries[secondary] = messages

		primaries[primary] = secondaries
	} else {
		var (
			primary   string
			secondary string
		)

		isPrimaryStatic := primaryPath.IsStatic()
		isSecondaryStatic := secondaryPath.IsStatic()

		if isPrimaryStatic {
			s, _, err := primaryPath.Eval(nil, true)
			if err != nil || len(s) == 0 {
				return nil
			}
			primary = s
		}

		if isSecondaryStatic {
			s, _, err := secondaryPath.Eval(nil, true)
			if err != nil {
				return nil
			}
			secondary = s
		}

		var (
			ok            bool
			secondaryList []string
			primaryMap    map[string][]string
		)

		primaries = make(map[string]map[string][]string)

		for _, msg := range messages {
			if msg != "" {
				var data interface{}

				err := json.Unmarshal([]byte(msg), &data)
				if err != nil {
					continue
				}

				if !isPrimaryStatic {
					primary, _, err = primaryPath.Eval(data, true)
					if err != nil || len(primary) == 0 {
						continue
					}
				}

				if !isSecondaryStatic {
					secondary, _, err = secondaryPath.Eval(data, true)
					if err != nil {
						continue
					}
				}

				primaryMap, ok = primaries[primary]
				if !ok || primaryMap == nil {
					primaryMap = make(map[string][]string)
					primaries[primary] = primaryMap
				}

				secondaryList, _ = primaryMap[secondary]
				primaryMap[secondary] = append(secondaryList, msg)
			}
		}
	}
	return primaries
}

func (oh *outHandler) waitComplete() {
	defer func() {
		recover()
		l := oh.GetLogger()
		if l != nil {
			l.Printf("Stoping '%s'...\n", oh.iotype)
		}
	}()

	l := oh.GetLogger()
	if l != nil {
		l.Printf("Starting '%s'...\n", oh.iotype)
	}

	<-oh.completed
}
