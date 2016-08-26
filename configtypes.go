package main

import "time"

type redisConfig struct {
	Compressed     bool   `json:"compressed"`
	PSubscribe     bool   `json:"psubscribe"`
	Server         string `json:"server"`
	Password       string `json:"password"`
	Channel        string `json:"channel"`
	MaxMessageSize int    `json:"maxMessageSize"`
}

type queueConfig struct {
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
	Count   int           `json:"count"`
	Size    int           `json:"size"`
	OnEvery time.Duration `json:"onEvery"`
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
	Queue     queueConfig   `json:"queue"`
	Buffer    bufferConfig  `json:"buffer"`
	Producers []inOutConfig `json:"producers"`
}

type outputsConfig struct {
	BulkCount        int           `json:"bulkCount"`
	MaxMessageSize   int           `json:"maxMessageSize"`
	TimestampKey     string        `json:"timestampKey"`
	TimestampFormat  string        `json:"timestampFormat"`
	Path             string        `json:"dataPath"`
	Pattern          string        `json:"dataPattern"`
	FlushOnEvery     time.Duration `json:"flushOnEvery"`
	SleepOnEvery     time.Duration `json:"sleepOnEvery"`
	SleepForMillisec time.Duration `json:"sleepForMillisec"`
	Queue            queueConfig   `json:"queue"`
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
