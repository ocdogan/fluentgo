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
	Server         string `json:"server"`
	Password       string `json:"password"`
	Channel        string `json:"channel"`
	MaxMessageSize int    `json:"maxMessageSize"`
}

type InQConfig struct {
	MaxCount int    `json:"maxCount"`
	MaxSize  uint64 `json:"maxSize"`
}

type LogConfig struct {
	Enabled     bool   `json:"enabled"`
	Console     bool   `json:"console"`
	Path        string `json:"path"`
	Type        string `json:"type"`
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
	Path            string      `json:"path"`
	Prefix          string      `json:"prefix"`
	Extension       string      `json:"extension"`
	MaxMessageSize  int         `json:"maxMessageSize"`
	TimestampKey    string      `json:"timestampKey"`
	TimestampFormat string      `json:"timestampFormat"`
	Flush           FlushConfig `json:"flush"`
}

type inOutParamConfig struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

type InOutConfig struct {
	Type   string             `json:"type"`
	Params []inOutParamConfig `json:"params"`
}

type InputsConfig struct {
	Queue       InQConfig     `json:"queue"`
	Buffer      BufferConfig  `json:"buffer"`
	OrphanFiles OrphanConfig  `json:"orphanFiles"`
	Producers   []InOutConfig `json:"producers"`
}

type OutQConfig struct {
	ChunkSize          int           `json:"chunkSize"`
	MaxCount           int           `json:"maxCount"`
	WaitPopForMillisec time.Duration `json:"waitPopForMillisec"`
}

type OutputsConfig struct {
	BulkCount        int           `json:"bulkCount"`
	MaxMessageSize   int           `json:"maxMessageSize"`
	TimestampKey     string        `json:"timestampKey"`
	TimestampFormat  string        `json:"timestampFormat"`
	Path             string        `json:"dir"`
	Pattern          string        `json:"dataPattern"`
	FlushOnEverySec  time.Duration `json:"flushOnEverySec"`
	SleepOnEverySec  time.Duration `json:"sleepOnEverySec"`
	SleepForMillisec time.Duration `json:"sleepForMillisec"`
	Queue            OutQConfig    `json:"queue"`
	Consumers        []InOutConfig `json:"consumers"`
	OrphanFiles      OrphanConfig  `json:"orphanFiles"`
}

type FluentConfig struct {
	MemProfile  string        `json:"memprofile"`
	CPUProfile  string        `json:"cpuprofile"`
	ProfileURL  string        `json:"profileURL"`
	ServiceMode string        `json:"serviceMode"`
	Log         LogConfig     `json:"log"`
	Inputs      InputsConfig  `json:"inputs"`
	Outputs     OutputsConfig `json:"outputs"`
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

	params := make(map[string]interface{}, len(cfg.Params))
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
