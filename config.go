package main

import (
	"encoding/json"
	"io/ioutil"
	"path"
)

func loadConfig(configPath string) *fluentConfig {
	if configPath == "" {
		configPath = path.Join(".", "config.json")
	}

	byt, err := ioutil.ReadFile(configPath)
	if err != nil {
		panic(err)
	}

	config := &fluentConfig{}
	if err := json.Unmarshal(byt, &config); err != nil {
		panic(err)
	}

	return config
}
