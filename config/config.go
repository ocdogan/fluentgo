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
	"encoding/json"
	"io/ioutil"
	"path"
)

var (
	currConfig string
	configs    map[string]*FluentConfig
)

func SetCurrentConfig(configFile string) {
	currConfig = configFile
}

func GetCurrentConfig() string {
	return currConfig
}

func LoadConfig(configFile string) *FluentConfig {
	if configFile == "" {
		configFile = path.Join(".", "config.json")
	}

	if configs != nil {
		cfg, _ := configs[configFile]
		if cfg != nil {
			return cfg
		}
	}

	byt, err := ioutil.ReadFile(configFile)
	if err != nil {
		panic(err)
	}

	config := &FluentConfig{}
	if err := json.Unmarshal(byt, &config); err != nil {
		panic(err)
	}

	if configs == nil {
		configs = make(map[string]*FluentConfig, 1)
	}
	configs[configFile] = config

	return config
}
