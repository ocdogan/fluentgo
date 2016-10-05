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

package config

import (
	"os"
	"strings"
	"time"

	"github.com/cloudfoundry/gosigar"
	"github.com/ocdogan/fluentgo/lib"
)

type RedisConfig struct {
	Compressed     bool   `json:"compressed"`
	PSubscribe     bool   `json:"psubscribe"`
	Server         string `json:"server,omitempty"`
	Password       string `json:"password,omitempty"`
	Channel        string `json:"channel,omitempty"`
	MaxMessageSize int    `json:"maxMessageSize"`
}

type InQConfig struct {
	MaxCount int    `json:"maxCount"`
	MaxSize  uint64 `json:"maxSize"`
}

type LogConfig struct {
	Enabled     bool   `json:"enabled"`
	Console     bool   `json:"console"`
	Path        string `json:"path,omitempty"`
	Type        string `json:"type,omitempty"`
	RollingSize int    `json:"rollingSize"`
}

type FlushConfig struct {
	Count      int           `json:"count"`
	Size       int           `json:"size"`
	OnEverySec time.Duration `json:"onEverySec"`
}

type OrphanConfig struct {
	DeleteAll       bool `json:"deleteAll"`
	ReplayLastXDays int  `json:"replayLastXDays"`
}

type BufferConfig struct {
	Path            string      `json:"path,omitempty"`
	Prefix          string      `json:"prefix,omitempty"`
	Extension       string      `json:"extension,omitempty"`
	MaxMessageSize  int         `json:"maxMessageSize"`
	TimestampKey    string      `json:"timestampKey,omitempty"`
	TimestampFormat string      `json:"timestampFormat,omitempty"`
	Flush           FlushConfig `json:"flush,omitempty"`
}

type inOutParamConfig struct {
	Name  string      `json:"name,omitempty"`
	Value interface{} `json:"value,omitempty"`
}

type InOutConfig struct {
	Type        string             `json:"type,omitempty"`
	Name        string             `json:"name,omitempty"`
	Description string             `json:"description,omitempty"`
	Params      []inOutParamConfig `json:"params,omitempty"`
}

type InputsConfig struct {
	Queue       InQConfig     `json:"queue,omitempty"`
	Buffer      BufferConfig  `json:"buffer,omitempty"`
	OrphanFiles OrphanConfig  `json:"orphanFiles,omitempty"`
	Producers   []InOutConfig `json:"producers,omitempty"`
}

type OutQConfig struct {
	ChunkSize          int           `json:"chunkSize"`
	MaxCount           int           `json:"maxCount"`
	WaitPopForMillisec time.Duration `json:"waitPopForMillisec"`
}

type OutputsConfig struct {
	BulkCount        int           `json:"bulkCount"`
	MaxMessageSize   int           `json:"maxMessageSize"`
	TimestampKey     string        `json:"timestampKey,omitempty"`
	TimestampFormat  string        `json:"timestampFormat,omitempty"`
	Path             string        `json:"dir,omitempty"`
	Pattern          string        `json:"dataPattern,omitempty"`
	FlushOnEverySec  time.Duration `json:"flushOnEverySec"`
	SleepOnEverySec  time.Duration `json:"sleepOnEverySec"`
	SleepForMillisec time.Duration `json:"sleepForMillisec"`
	Queue            OutQConfig    `json:"queue,omitempty"`
	Consumers        []InOutConfig `json:"consumers,omitempty"`
	OrphanFiles      OrphanConfig  `json:"orphanFiles,omitempty"`
}

type TLSConfig struct {
	CertFile  string `json:"certFile,omitempty"`
	KeyFile   string `json:"keyFile,omitempty"`
	CAFile    string `json:"caFile,omitempty"`
	VerifySsl bool   `json:"verifySsl,omitempty"`
}

type AdminConfig struct {
	Enabled     bool      `json:"enabled"`
	HTTPAddress string    `json:"httpAddress,omitempty"`
	TLS         TLSConfig `json:"tls,omitempty"`
}

type FluentConfig struct {
	MemProfile  string        `json:"memprofile,omitempty"`
	CPUProfile  string        `json:"cpuprofile,omitempty"`
	ProfileURL  string        `json:"profileURL,omitempty"`
	ServiceMode string        `json:"serviceMode,omitempty"`
	Log         LogConfig     `json:"log,omitempty"`
	Inputs      InputsConfig  `json:"inputs,omitempty"`
	Outputs     OutputsConfig `json:"outputs,omitempty"`
	Admin       AdminConfig   `json:"admin,omitempty"`
}

func (cfg *BufferConfig) GetPath() string {
	var dir string
	if cfg != nil {
		dir = strings.TrimSpace(cfg.Path)
	}

	if dir == "" || dir == "." {
		dir = "." + string(os.PathSeparator) +
			"buffer" + string(os.PathSeparator)
	}
	return lib.PreparePath(dir)
}

func (cfg *BufferConfig) GetFlushCount() int {
	var cnt int
	if cfg != nil {
		cnt = cfg.Flush.Count
	}

	if cnt < lib.MinBufferCount {
		cnt = lib.MinBufferCount
	} else if cnt > lib.MaxBufferCount {
		cnt = lib.MaxBufferCount
	}
	return cnt
}

func (cfg *BufferConfig) GetFlushSize() int {
	var sz int
	if cfg != nil {
		sz = cfg.Flush.Size
	}

	if sz < lib.MinBufferSize {
		sz = lib.MinBufferSize
	} else if sz > lib.MaxBufferSize {
		sz = lib.MaxBufferSize
	}
	return sz
}

func (cfg *BufferConfig) GetFlushOnEverySec() time.Duration {
	var val int64
	if cfg != nil {
		val = int64(cfg.Flush.OnEverySec)
	}

	return time.Duration(lib.MinInt64(600, lib.MaxInt64(2, val))) * time.Second
}

func (cfg *BufferConfig) GetExtension() string {
	var ext string
	if cfg != nil {
		ext = strings.TrimSpace(cfg.Extension)
	}

	if ext == "" {
		ext = ".buf"
	} else if ext[0] != '.' {
		ext = "." + ext
	}
	return ext
}

func (cfg *BufferConfig) GetPrefix() string {
	var prefix string
	if cfg != nil {
		prefix = strings.TrimSpace(cfg.Prefix)
	}

	if prefix == "" {
		prefix = "bf-"
	} else if prefix[len(prefix)-1] != '-' {
		prefix += "-"
	}
	return prefix
}

func (cfg *BufferConfig) GetMaxMessageSize() int {
	return lib.MinInt(lib.InvalidMessageSize, lib.MaxInt(-1, cfg.MaxMessageSize))
}

func (cfg *BufferConfig) GetTimestampFormat() string {
	if cfg != nil && strings.TrimSpace(cfg.TimestampKey) != "" {
		tsFormat := strings.TrimSpace(cfg.TimestampFormat)
		if tsFormat == "" {
			tsFormat = lib.ISO8601Time
		}
		return tsFormat
	}
	return ""
}

func (cfg *OutputsConfig) GetDataPath(inCfg *InputsConfig) string {
	var dir string
	if cfg != nil {
		dir = strings.TrimSpace(cfg.Path)
	}

	if dir == "" && inCfg != nil {
		dir = (&inCfg.Buffer).GetPath()
		if dir != "" {
			dir += "completed" + string(os.PathSeparator)
		}
	}

	if dir == "" || dir == "." {
		return "." + string(os.PathSeparator) +
			"buffer" + string(os.PathSeparator) +
			"completed" + string(os.PathSeparator)
	}

	return lib.PreparePath(dir)
}

func (cfg *OutputsConfig) GetBulkCount() int {
	var bulkCount int
	if cfg != nil {
		bulkCount = cfg.BulkCount
	}

	if bulkCount < 1 {
		bulkCount = lib.DefaultOutBulkCount
	}
	return lib.MinInt(500, bulkCount)
}

func (cfg *OutputsConfig) GetMaxMessageSize() int {
	var val int
	if cfg != nil {
		val = cfg.MaxMessageSize
	}

	return lib.MaxInt(-1, val)
}

func (cfg *OutputsConfig) GetFlushOnEverySec() time.Duration {
	var val int64
	if cfg != nil {
		val = int64(cfg.FlushOnEverySec)
	}

	return time.Duration(lib.MinInt64(600, lib.MaxInt64(2, val))) * time.Second
}

func (cfg *OutputsConfig) GetSleepOnEverySec() time.Duration {
	var val int64
	if cfg != nil {
		val = int64(cfg.SleepOnEverySec)
	}

	return time.Duration(lib.MinInt64(60000, lib.MaxInt64(1, val))) * time.Second
}

func (cfg *OutputsConfig) GetSleepForMillisec() time.Duration {
	var val int64
	if cfg != nil {
		val = int64(cfg.SleepForMillisec)
	}

	return time.Duration(lib.MinInt64(30000, lib.MaxInt64(1, val))) * time.Millisecond
}

func (cfg *OutputsConfig) GetDataPattern(inCfg *InputsConfig) string {
	var pattern string
	if cfg != nil {
		pattern = strings.TrimSpace(cfg.Pattern)
	}

	if pattern == "" && inCfg != nil {
		pattern = (&inCfg.Buffer).GetPrefix() +
			"*" + (&inCfg.Buffer).GetExtension()
	}
	return pattern
}

func (cfg *OutputsConfig) GetTimestampFormat() string {
	tsFormat := ""
	if cfg != nil && strings.TrimSpace(cfg.TimestampKey) != "" {
		tsFormat = strings.TrimSpace(cfg.TimestampFormat)
		if tsFormat == "" {
			tsFormat = lib.ISO8601Time
		}
		return tsFormat
	}
	return ""
}

func (cfg *InOutConfig) GetParamsMap() map[string]interface{} {
	if cfg == nil {
		return make(map[string]interface{}, 0)
	}

	params := make(map[string]interface{}, len(cfg.Params)+2)

	name := strings.TrimSpace(cfg.Name)
	if name != "" {
		params["@name"] = name
	}

	description := strings.TrimSpace(cfg.Description)
	if description != "" {
		params["@description"] = description
	}

	for _, p := range cfg.Params {
		params[p.Name] = p.Value
	}
	return params
}

func (cfg *InQConfig) GetMaxParams() (maxCount int, maxSize uint64) {
	maxSize = 0
	maxCount = 10000

	if cfg != nil {
		maxSize = cfg.MaxSize

		maxCount = cfg.MaxCount
		if maxCount <= 0 {
			maxCount = 10000
		}
	}

	if maxSize == 0 {
		m := sigar.Mem{}
		if e := m.Get(); e == nil {
			maxSize = uint64(2 * uint64(m.Total/3))
		}
	}
	return
}

func (cfg *OutQConfig) GetParams() (chunkSize, maxCount int, waitPopForMillisec time.Duration) {
	if cfg != nil {
		chunkSize = cfg.ChunkSize
		maxCount = cfg.MaxCount
		waitPopForMillisec = cfg.WaitPopForMillisec
	}
	return
}

func (cfg *LogConfig) GetPath() string {
	var dir string
	if cfg != nil {
		dir = strings.TrimSpace(cfg.Path)
	}

	if dir == "" || dir == "." {
		dir = "." + string(os.PathSeparator) +
			"log" + string(os.PathSeparator)
	}

	return lib.PreparePath(dir)
}

func (cfg *OrphanConfig) GetDeleteAll() bool {
	if cfg != nil {
		return cfg.DeleteAll
	}
	return false
}

func (cfg *OrphanConfig) GetReplayLastXDays() int {
	var val int
	if cfg != nil {
		val = cfg.ReplayLastXDays
	}
	return lib.MinInt(365, lib.MaxInt(-1, val))
}
