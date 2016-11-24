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
	"crypto/tls"

	"github.com/ocdogan/fluentgo/config"
	"github.com/ocdogan/fluentgo/lib"
)

type tlsIO struct {
	secure      bool
	verifySsl   bool
	certFile    string
	keyFile     string
	caFile      string
	tlsConfig   *tls.Config
	loadTLSFunc func() (secure bool, config *tls.Config, err error)
}

func newTLSIO(manager InOutManager, params map[string]interface{}) *tlsIO {
	var (
		ok        bool
		verifySsl bool
		caFile    string
		certFile  string
		keyFile   string
	)

	certFile, ok = config.ParamAsString(params, "certFile")
	if ok && certFile != "" {
		certFile = lib.PrepareFile(certFile)
		if certFile != "" {
			keyFile, ok = config.ParamAsString(params, "keyFile")
			if ok && keyFile != "" {
				keyFile = lib.PrepareFile(keyFile)
				if keyFile != "" {
					verifySsl, _ = config.ParamAsBool(params, "verifySsl")

					caFile, ok = config.ParamAsString(params, "caFile")
					if ok && caFile != "" {
						caFile = lib.PrepareFile(caFile)
					}
				}
			}
		}
	}

	return &tlsIO{
		caFile:    caFile,
		certFile:  certFile,
		keyFile:   keyFile,
		verifySsl: verifySsl,
	}
}

func (wio *tlsIO) loadCert() error {
	tlsLoaded := false
	var config *tls.Config

	defer func() {
		if tlsLoaded {
			wio.tlsConfig = config
			wio.secure = true
		} else {
			wio.secure = false
			wio.tlsConfig = nil
		}
	}()

	if wio.loadTLSFunc != nil {
		var err error
		tlsLoaded, config, err = wio.loadTLSFunc()

		if err != nil {
			tlsLoaded, config = false, nil
			return err
		}
	}
	return nil
}
