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
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/ocdogan/fluentgo/config"
	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/log"
)

type baseIO struct {
	id           lib.UUID
	name         string
	description  string
	enabled      bool
	iotype       string
	processing   int32
	compressed   bool
	params       map[string]interface{}
	compressType lib.CompressionType
	logger       log.Logger
	manager      InOutManager
}

func newBaseIO(manager InOutManager, params map[string]interface{}) *baseIO {
	id, err := lib.NewUUID()
	if err != nil {
		return nil
	}

	name := ""
	description := ""

	enabled := true
	compressed := false
	compressType := lib.CtGZip

	var ok bool
	if params != nil {
		enabled, ok = config.ParamAsBool(params, "enabled")
		if !ok {
			enabled = true
		}

		compressed, _ = config.ParamAsBool(params, "compressed")

		var s string
		s, ok = config.ParamAsString(params, "compressType")
		if ok && s != "" {
			s = strings.ToLower(s)
			if s == "zip" {
				compressType = lib.CtZip
			}
		}

		name, _ = config.ParamAsString(params, "@name")
		description, _ = config.ParamAsString(params, "@description")
	}

	return &baseIO{
		id:           *id,
		name:         name,
		description:  description,
		enabled:      enabled,
		compressed:   compressed,
		compressType: compressType,
		params:       params,
		manager:      manager,
		logger:       manager.GetLogger(),
	}
}

func (bio *baseIO) Name() string {
	return bio.name
}

func (bio *baseIO) SetName(name string) {
	bio.name = strings.TrimSpace(name)
}

func (bio *baseIO) Description() string {
	return bio.description
}

func (bio *baseIO) SetDescription(description string) {
	bio.description = strings.TrimSpace(description)
}

func (bio *baseIO) ID() lib.UUID {
	return bio.id
}

func (bio *baseIO) Compressed() bool {
	return bio.compressed
}

func (bio *baseIO) CompressType() lib.CompressionType {
	return bio.compressType
}

func (bio *baseIO) GetLogger() log.Logger {
	m := bio.manager
	if m != nil {
		return m.GetLogger()
	}
	return nil
}

func (bio *baseIO) GetManager() InOutManager {
	return bio.manager
}

func (bio *baseIO) Enabled() bool {
	return bio.enabled
}

func (bio *baseIO) GetIOType() string {
	return bio.iotype
}

func (bio *baseIO) Processing() bool {
	return atomic.LoadInt32(&bio.processing) != 0
}

func (bio *baseIO) SetProcessing(ok bool) {
	if ok {
		atomic.StoreInt32(&bio.processing, 1)
	} else {
		atomic.StoreInt32(&bio.processing, 0)
	}
}

func (bio *baseIO) GetParameters() map[string]interface{} {
	return bio.params
}

func (bio *baseIO) InformStart() {
	recover()
	l := bio.GetLogger()
	if l != nil {
		l.Printf("* Starting '%s'...\n", bio.iotype)
	}
}

func (bio *baseIO) InformStop() {
	recover()
	l := bio.GetLogger()
	if l != nil {
		l.Printf("* Stoping '%s'...\n", bio.iotype)
	}
}

func (bio *baseIO) InformParameters() {
	recover()

	params := bio.GetParameters()
	if params != nil {
		bytes, err := json.Marshal(params)
		if err == nil {
			l := bio.GetLogger()
			if l == nil {
				fmt.Printf("* %s parameters: %v\n", bio.iotype, string(bytes))
			} else {
				l.Printf("* %s parameters: %v\n", bio.iotype, string(bytes))
			}
		}
	}
}
