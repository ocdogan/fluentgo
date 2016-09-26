package config

import (
	"encoding/json"
	"io/ioutil"
	"path"
)

func LoadConfig(configPath string) *FluentConfig {
	if configPath == "" {
		configPath = path.Join(".", "config.json")
	}

	byt, err := ioutil.ReadFile(configPath)
	if err != nil {
		panic(err)
	}

	config := &FluentConfig{}
	if err := json.Unmarshal(byt, &config); err != nil {
		panic(err)
	}

	return config
}
