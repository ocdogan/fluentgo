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

package http

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/buaazp/fasthttprouter"
	"github.com/ocdogan/fluentgo/config"
	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/log"
	"github.com/valyala/fasthttp"
)

type HttpRouter struct {
	fasthttprouter.Router
}

type HttpServer struct {
	net.Listener
	fasthttp.Server
	state      uint32
	addr       string
	logger     log.Logger
	certConfig *tls.Config
	routing    *HttpRouter
}

func NewHttpServer(routing *HttpRouter, logger log.Logger, params map[string]interface{}) (*HttpServer, error) {
	if routing == nil {
		return nil, fmt.Errorf("Routing cannot be nil.")
	}

	var (
		ok        bool
		addr      string
		certFile  string
		keyFile   string
		caFile    string
		verifySsl bool
	)

	addr, ok = config.ParamAsString(params, "addr")
	if !ok || addr == "" {
		addr = ":8080"
	}

	if certFile, ok := config.ParamAsString(params, "certFile"); ok {
		certFile = lib.PrepareFile(certFile)
		if certFile != "" {
			if keyFile, ok := config.ParamAsString(params, "keyFile"); ok {
				keyFile = lib.PrepareFile(keyFile)
				if keyFile != "" {
					verifySsl, _ = config.ParamAsBool(params, "verifySsl")
					if caFile, ok := config.ParamAsString(params, "caFile"); ok {
						caFile = lib.PrepareFile(caFile)
					}
				}
			}
		}
	}

	certConfig, err := lib.LoadClientCert(certFile, keyFile, caFile, verifySsl)
	if err != nil {
		if logger != nil {
			logger.Printf("Error loading certification files '%s', '%s', %v", certFile, keyFile, err)
		}
		return nil, err
	}

	server := &HttpServer{
		Server: fasthttp.Server{
			Handler: routing.Handler,
		},
		addr:       addr,
		certConfig: certConfig,
		logger:     logger,
		routing:    routing,
	}

	return server, nil
}

func SetRestError(ctx *fasthttp.RequestCtx, err error, errCode int) {
	ctx.Response.Header.SetContentType("application/json")

	restErr := &RestError{
		Method:      lib.BytesToString(ctx.Method()),
		ErrorString: fmt.Sprint(err),
		ErrorCode:   errCode,
		Arguments:   RestErrorArgs{Query: lib.BytesToString(ctx.URI().QueryString())},
	}

	ctx.VisitUserValues(func(key []byte, value interface{}) {
		restErr.Arguments.Keys = append(restErr.Arguments.Keys,
			RestErrorArg{
				Key:   string(key),
				Value: fmt.Sprintf("%s", value),
			})
	})

	data, err := json.Marshal(restErr)
	ctx.Error(lib.BytesToString(data), errCode)
}

func NotFound(ctx *fasthttp.RequestCtx) {
	SetRestError(ctx, fmt.Errorf("Not found"), fasthttp.StatusNotFound)
}

func PanicHandler(ctx *fasthttp.RequestCtx, rcv interface{}) {
	SetRestError(ctx, fmt.Errorf("%s", rcv), 503)
}

func MethodNotAllowed(ctx *fasthttp.RequestCtx) {
	SetRestError(ctx, fmt.Errorf("Not allowed"), fasthttp.StatusMethodNotAllowed)
}

/*
func (h *HttpServer) Handle(method, path string, handle fasthttprouter.Handle) {
	if h != nil {
		h.routing.Handle(method, path, handle)
	}
}
*/

func (h *HttpServer) Start(quitSignal <-chan bool) error {
	if !atomic.CompareAndSwapUint32(&h.state, 0, 1) {
		return fmt.Errorf("Http server is already running.")
	}
	defer func() {
		recover()
		atomic.StoreUint32(&h.state, 0)
	}()

	var (
		err error
		ln  net.Listener
	)

	if h.logger != nil {
		h.logger.Printf("Starting admin module at: '%s'\n", h.addr)
	}

	if h.certConfig == nil {
		ln, err = net.Listen("tcp4", h.addr)
	} else {
		ln, err = tls.Listen("tcp4", h.addr, h.certConfig)
	}

	if err != nil {
		if h.logger != nil {
			h.logger.Printf("Error running Http server at: '%s', %v", h.addr, err)
		}
		return err
	}

	h.Listener = ln

	go func(h *HttpServer) {
		defer func() {
			defer func() {
				if h.logger != nil {
					h.logger.Println("Stopping admin module...")
				}
			}()
		}()

		err = h.Serve(h.Listener)
		if err != nil && h.logger != nil {
			h.logger.Printf("Error running Http server at: %s, %v", h.addr, err)
		}
	}(h)

	go func(h *HttpServer) {
		<-quitSignal
		h.Stop()
	}(h)

	return err
}

func (h *HttpServer) Stop() error {
	if h == nil || atomic.LoadUint32(&h.state) != 1 {
		return nil
	}

	defer func() {
		recover()
		atomic.StoreUint32(&h.state, 0)
	}()

	return h.Close()
}
