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
	"net"
	"strings"

	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/lib/config"
	"github.com/ocdogan/fluentgo/lib/log"
)

type tcpIO struct {
	id          lib.UUID
	compressed  bool
	host        string
	secure      bool
	certFile    string
	keyFile     string
	tlsConfig   *tls.Config
	logger      log.Logger
	loadTLSFunc func() (secure bool, config *tls.Config, err error)
}

func newTCPIO(manager InOutManager, config *config.InOutConfig) *tcpIO {
	if config == nil {
		return nil
	}

	id, err := lib.NewUUID()
	if err != nil {
		return nil
	}

	params := config.GetParamsMap()

	var (
		ok   bool
		s    string
		host string
	)

	if s, ok = params["host"].(string); ok {
		host = strings.TrimSpace(s)
	}
	if host == "" {
		return nil
	}

	var (
		compressed bool
		certFile   string
		keyFile    string
	)

	compressed, ok = params["compressed"].(bool)

	certFile, ok = params["certFile"].(string)
	if ok {
		certFile = lib.PrepareFile(certFile)
		if certFile != "" {
			keyFile, ok = params["keyFile"].(string)
			if ok {
				keyFile = lib.PrepareFile(keyFile)
			}
		}
	}

	tio := &tcpIO{
		id:         *id,
		host:       host,
		compressed: compressed,
		logger:     manager.GetLogger(),
	}

	return tio
}

func (tio *tcpIO) ID() lib.UUID {
	return tio.id
}

func (tio *tcpIO) tryToCloseConn(conn net.Conn) error {
	var closeErr error
	if conn != nil {
		defer func() {
			err := recover()
			if closeErr == nil {
				closeErr, _ = err.(error)
			}
		}()
		closeErr = conn.Close()
	}
	return closeErr
}

func (tio *tcpIO) loadCert() error {
	tlsLoaded := false
	var config *tls.Config

	defer func() {
		if tlsLoaded {
			tio.tlsConfig = config
			tio.secure = true
		} else {
			tio.secure = false
			tio.tlsConfig = nil
		}
	}()

	if tio.loadTLSFunc != nil {
		var err error
		tlsLoaded, config, err = tio.loadTLSFunc()

		if err != nil {
			tlsLoaded, config = false, nil

			l := tio.logger
			if l != nil {
				l.Print(err)
			}

			return err
		}
	}
	return nil
}
