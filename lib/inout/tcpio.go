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
		host:       host,
		compressed: compressed,
		logger:     manager.GetLogger(),
	}

	return tio
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
