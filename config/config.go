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
	"strings"
	"time"
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

func ParamAsBool(params map[string]interface{}, param string) (result bool, ok bool) {
	if params != nil {
		result, ok = params[param].(bool)
	}
	return
}

func ParamAsDuration(params map[string]interface{}, param string) (result time.Duration, ok bool) {
	if params != nil {
		var f float64
		f, ok = params[param].(float64)
		if ok {
			result = time.Duration(f)
		}
	}
	return
}

func ParamAsDurationLimited(params map[string]interface{}, param string, min time.Duration, max time.Duration) (result time.Duration, ok bool) {
	if params != nil {
		var f float64
		f, ok = params[param].(float64)
		if ok {
			result = time.Duration(f)
			if result < min {
				result = min
			}
			if result > max && max > min {
				result = max
			}
		}
	}
	return
}

func ParamAsFloat(params map[string]interface{}, param string) (result float64, ok bool) {
	if params != nil {
		result, ok = params[param].(float64)
	}
	return
}

func ParamAsFloatLimited(params map[string]interface{}, param string, min float64, max float64) (result float64, ok bool) {
	if params != nil {
		result, ok = params[param].(float64)
		if ok {
			if result < min {
				result = min
			}
			if result > max && max > min {
				result = max
			}
		}
	}
	return
}

func ParamAsInt(params map[string]interface{}, param string) (result int, ok bool) {
	if params != nil {
		var f float64
		f, ok = params[param].(float64)
		if ok {
			result = int(f)
		}
	}
	return
}

func ParamAsIntLimited(params map[string]interface{}, param string, min int, max int) (result int, ok bool) {
	if params != nil {
		var f float64
		f, ok = params[param].(float64)
		if ok {
			result = int(f)
			if result < min {
				result = min
			}
			if result > max && max > min {
				result = max
			}
		}
	}
	return
}

func ParamAsInt32(params map[string]interface{}, param string) (result int32, ok bool) {
	if params != nil {
		var f float64
		f, ok = params[param].(float64)
		if ok {
			result = int32(f)
		}
	}
	return
}

func ParamAsInt32Limited(params map[string]interface{}, param string, min int32, max int32) (result int32, ok bool) {
	if params != nil {
		var f float64
		f, ok = params[param].(float64)
		if ok {
			result = int32(f)
			if result < min {
				result = min
			}
			if result > max && max > min {
				result = max
			}
		}
	}
	return
}

func ParamAsInt64(params map[string]interface{}, param string) (result int64, ok bool) {
	if params != nil {
		var f float64
		f, ok = params[param].(float64)
		if ok {
			result = int64(f)
		}
	}
	return
}

func ParamAsInt64Limited(params map[string]interface{}, param string, min int64, max int64) (result int64, ok bool) {
	if params != nil {
		var f float64
		f, ok = params[param].(float64)
		if ok {
			result = int64(f)
			if result < min {
				result = min
			}
			if result > max && max > min {
				result = max
			}
		}
	}
	return
}

func ParamAsString(params map[string]interface{}, param string) (result string, ok bool) {
	if params != nil {
		result, ok = params[param].(string)
		if ok {
			result = strings.TrimSpace(result)
		}
	}
	return
}
