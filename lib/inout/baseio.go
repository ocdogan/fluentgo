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
	"sync/atomic"

	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/lib/log"
)

type baseIO struct {
	id           lib.UUID
	name         string
	description  string
	enabled      bool
	iotype       string
	processing   int32
	compressed   bool
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
		enabled, ok = params["enabled"].(bool)
		if !ok {
			enabled = true
		}

		compressed, _ = params["compressed"].(bool)

		var s string
		if s, ok = params["compressType"].(string); ok {
			s = strings.TrimSpace(s)
			if s != "" {
				s = strings.ToLower(s)
				if s == "zip" {
					compressType = lib.CtZip
				}
			}
		}

		if name, ok = params["@name"].(string); ok {
			name = strings.TrimSpace(s)
		}

		if description, ok = params["@description"].(string); ok {
			description = strings.TrimSpace(s)
		}
	}

	return &baseIO{
		id:           *id,
		name:         name,
		description:  description,
		enabled:      enabled,
		compressed:   compressed,
		compressType: compressType,
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
