package main

import (
	"os"
	"strings"
	"time"

	"github.com/cloudfoundry/gosigar"
)

type redisConfig struct {
	Compressed     bool   `json:"compressed"`
	PSubscribe     bool   `json:"psubscribe"`
	Server         string `json:"server"`
	Password       string `json:"password"`
	Channel        string `json:"channel"`
	MaxMessageSize int    `json:"maxMessageSize"`
}

type inQConfig struct {
	MaxCount int    `json:"maxCount"`
	MaxSize  uint64 `json:"maxSize"`
}

type logConfig struct {
	Enabled     bool   `json:"enabled"`
	Console     bool   `json:"console"`
	Path        string `json:"path"`
	Type        string `json:"type"`
	RollingSize int    `json:"rollingSize"`
}

type flushConfig struct {
	Count      int           `json:"count"`
	Size       int           `json:"size"`
	OnEverySec time.Duration `json:"onEverySec"`
}

type bufferConfig struct {
	Path            string      `json:"path"`
	Prefix          string      `json:"prefix"`
	Extension       string      `json:"extension"`
	MaxMessageSize  int         `json:"maxMessageSize"`
	TimestampKey    string      `json:"timestampKey"`
	TimestampFormat string      `json:"timestampFormat"`
	Flush           flushConfig `json:"flush"`
}

type inOutParamConfig struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

type inOutConfig struct {
	Type   string             `json:"type"`
	Params []inOutParamConfig `json:"params"`
}

type inputsConfig struct {
	Queue     inQConfig     `json:"queue"`
	Buffer    bufferConfig  `json:"buffer"`
	Producers []inOutConfig `json:"producers"`
}

type outQConfig struct {
	ChunkSize          int           `json:"chunkSize"`
	MaxCount           int           `json:"maxCount"`
	WaitPopForMillisec time.Duration `json:"waitPopForMillisec"`
}

type outputsConfig struct {
	BulkCount        int           `json:"bulkCount"`
	MaxMessageSize   int           `json:"maxMessageSize"`
	TimestampKey     string        `json:"timestampKey"`
	TimestampFormat  string        `json:"timestampFormat"`
	Path             string        `json:"dir"`
	Pattern          string        `json:"dataPattern"`
	FlushOnEverySec  time.Duration `json:"flushOnEverySec"`
	SleepOnEverySec  time.Duration `json:"sleepOnEverySec"`
	SleepForMillisec time.Duration `json:"sleepForMillisec"`
	Queue            outQConfig    `json:"queue"`
	Consumers        []inOutConfig `json:"consumers"`
}

type fluentConfig struct {
	MemProfile  string        `json:"memprofile"`
	CPUProfile  string        `json:"cpuprofile"`
	ProfileURL  string        `json:"profileURL"`
	ServiceMode string        `json:"serviceMode"`
	Log         logConfig     `json:"log"`
	Inputs      inputsConfig  `json:"inputs"`
	Outputs     outputsConfig `json:"outputs"`
}

func (cfg *bufferConfig) getPath() string {
	var dir string
	if cfg != nil {
		dir = strings.TrimSpace(cfg.Path)
	}

	if dir == "" || dir == "." {
		dir = "." + string(os.PathSeparator) +
			"buffer" + string(os.PathSeparator)
	}
	return preparePath(dir)
}

func (cfg *bufferConfig) getMaxCount() int {
	var cnt int
	if cfg != nil {
		cnt = cfg.Flush.Count
	}

	if cnt < minBufferCount {
		cnt = minBufferCount
	} else if cnt > maxBufferCount {
		cnt = maxBufferCount
	}
	return cnt
}

func (cfg *bufferConfig) getExtension() string {
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

func (cfg *bufferConfig) getPrefix() string {
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

func (cfg *bufferConfig) getMaxSize() int {
	var sz int
	if cfg != nil {
		sz = cfg.Flush.Size
	}

	if sz < minBufferSize {
		sz = minBufferSize
	} else if sz > maxBufferSize {
		sz = maxBufferSize
	}
	return sz
}

func (cfg *bufferConfig) getMaxMessageSize() int {
	return minInt(InvalidMessageSize, maxInt(-1, cfg.MaxMessageSize))
}

func (cfg *bufferConfig) getTimestampFormat() string {
	if strings.TrimSpace(cfg.TimestampKey) != "" {
		tsFormat := strings.TrimSpace(cfg.TimestampFormat)
		if tsFormat == "" {
			tsFormat = ISO8601Time
		}
		return tsFormat
	}
	return ""
}

func (cfg *outputsConfig) getDataPath(inCfg *inputsConfig) string {
	var dir string
	if cfg != nil {
		dir = strings.TrimSpace(cfg.Path)
	}

	if dir == "" && inCfg != nil {
		dir = (&inCfg.Buffer).getPath()
		if dir != "" {
			dir += "completed" + string(os.PathSeparator)
		}
	}

	if dir == "" || dir == "." {
		return "." + string(os.PathSeparator) +
			"buffer" + string(os.PathSeparator) +
			"completed" + string(os.PathSeparator)
	}

	return preparePath(dir)
}

func (cfg *outputsConfig) getBulkCount() int {
	bulkCount := cfg.BulkCount
	if bulkCount < 1 {
		bulkCount = defaultOutBulkCount
	}
	return minInt(500, bulkCount)
}

func (cfg *outputsConfig) getMaxMessageSize() int {
	return maxInt(cfg.MaxMessageSize, -1)
}

func (cfg *outputsConfig) getFlushOnEverySec() time.Duration {
	return time.Duration(minInt64(600, maxInt64(2, int64(cfg.FlushOnEverySec)))) * time.Second
}

func (cfg *outputsConfig) getSleepOnEverySec() time.Duration {
	return time.Duration(minInt64(60000, maxInt64(1, int64(cfg.SleepOnEverySec)))) * time.Second
}

func (cfg *outputsConfig) getSleepForMillisec() time.Duration {
	return time.Duration(minInt64(30000, maxInt64(1, int64(cfg.SleepForMillisec)))) * time.Millisecond
}

func (cfg *outputsConfig) getDataPattern(inCfg *inputsConfig) string {
	var pattern string
	if cfg != nil {
		pattern = strings.TrimSpace(cfg.Pattern)
	}

	if pattern == "" && inCfg != nil {
		pattern = (&inCfg.Buffer).getPrefix() +
			"*" + (&inCfg.Buffer).getExtension()
	}
	return pattern
}

func (cfg *outputsConfig) getTimestampFormat() string {
	tsFormat := ""
	if strings.TrimSpace(cfg.TimestampKey) != "" {
		tsFormat = strings.TrimSpace(cfg.TimestampFormat)
		if tsFormat == "" {
			tsFormat = ISO8601Time
		}
		return tsFormat
	}
	return ""
}

func (cfg *inOutConfig) getParamsMap() map[string]interface{} {
	if cfg == nil {
		return make(map[string]interface{}, 0)
	}

	params := make(map[string]interface{}, len(cfg.Params))
	for _, p := range cfg.Params {
		params[p.Name] = p.Value
	}
	return params
}

func (cfg *inQConfig) getMaxParams() (maxCount int, maxSize uint64) {
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

func (cfg *outQConfig) getParams() (chunkSize, maxCount int, waitPopForMillisec time.Duration) {
	if cfg != nil {
		chunkSize = cfg.ChunkSize
		maxCount = cfg.MaxCount
		waitPopForMillisec = cfg.WaitPopForMillisec
	}
	return
}

func (cfg *logConfig) getPath() string {
	var dir string
	if cfg != nil {
		dir = strings.TrimSpace(cfg.Path)
	}

	if dir == "" || dir == "." {
		dir = "." + string(os.PathSeparator) +
			"log" + string(os.PathSeparator)
	}

	return preparePath(dir)
}

func (cfg *flushConfig) getOnEverySec() time.Duration {
	return time.Duration(minInt64(600, maxInt64(30, int64(cfg.OnEverySec)))) * time.Second
}
