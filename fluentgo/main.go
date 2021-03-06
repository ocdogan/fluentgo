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

package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ocdogan/fluentgo/config"
	httpsrv "github.com/ocdogan/fluentgo/http"
	"github.com/ocdogan/fluentgo/inout"
	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/log"
	"github.com/ocdogan/fluentgo/profiler"
)

var (
	ioman       *inout.InAndOuts
	configPath  = flag.String("config", "", "The config.json file fully qualified path.")
	profileURL  = flag.String("profileURL", "", "Http endpoint serving profile data.")
	cpuproFile  = flag.String("cpuprofile", "", "write CPU profile to file")
	memproFile  = flag.String("memprofile", "", "write mem profile to file")
	servicemode = flag.String("servicemode", "", "defines how fluentgo service will act [pub|sub|pubsub]")
)

func main() {
	defer func() {
		recover()
		fmt.Println("* Stopping application...")
	}()
	fmt.Println("* Starting application...")

	flag.Parse()

	quitSignal := waitForQuit()

	config.SetCurrentConfig(*configPath)

	config := config.LoadConfig(*configPath)
	logger := log.NewLogger(&config.Log)

	profile(config, logger, quitSignal)
	start(config, logger, quitSignal)
}

func waitForQuit() (quitSignal <-chan bool) {
	qch := make(chan bool)
	quitSignal = qch

	go func(quitSignal chan<- bool) {
		defer func() {
			fmt.Println("* Termination signalled...")
			close(quitSignal)
		}()

		sch := make(chan os.Signal, 1)
		signal.Notify(sch, os.Interrupt)
		signal.Notify(sch, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGQUIT)

		<-sch
	}(qch)

	return quitSignal
}

func start(config *config.FluentConfig, logger log.Logger, quitSignal <-chan bool) {
	mode, smode, ok := getServiceMode(config)

	fmt.Printf("* Service mode: %s\n", smode)
	if !ok {
		msg := fmt.Sprintf("Invalid service mode, setting service mode to 'inout'.")
		logger.Println(msg)
	}

	startAdminModule(config, logger, quitSignal)

	ioman = inout.NewInOutManager(mode, config, logger, quitSignal)
	if ioman != nil {
		ioman.Process()
	}
}

func profile(config *config.FluentConfig, logger log.Logger, quitSignal <-chan bool) {
	mpFile := getMemProFile(config)
	cpFile := getCPUProFile(config)

	profURL := getProfileURL(config)
	if profURL != "" {
		go func() {
			defer recover()
			logger.Println(http.ListenAndServe(profURL, nil))
		}()
	}

	profiler.ScheduleMemProfiler(mpFile, logger, quitSignal)

	fn := profiler.ScheduleCPUProfiler(cpFile, logger)
	if fn != nil {
		defer func() {
			defer recover()
			fn()
		}()
	}
}

func getProfileURL(config *config.FluentConfig) string {
	url := *profileURL
	if url == "" {
		url = config.ProfileURL
	}
	return strings.TrimSpace(url)
}

func getRandomProfileName(profile, extension string) (result string) {
	if !(profile == "?" || profile == "*") {
		return profile
	}

	if extension == "" {
		extension = ".prof"
	} else if extension[0] != '.' {
		extension = "." + extension
	}

	defer recover()

	dir, process := filepath.Split(os.Args[0])

	var err error
	dir, err = filepath.Abs(dir)
	if err != nil {
		return ""
	}

	ext := filepath.Ext(process)
	if ext != "" {
		process = process[0 : len(process)-len(ext)]
	}

	filenames, err := filepath.Glob(process + "_*" + extension)
	if err != nil {
		return ""
	}

	index := -1

	if len(filenames) == 0 {
		return fmt.Sprintf("%s_%06d%s", process, 1, extension)
	}

	filesMap := make(map[string]string, len(filenames))

	for _, filename := range filenames {
		pad := filename[len(process) : len(filename)-len(extension)]
		if pad != "" {
			filesMap[pad] = filename
		}
	}

	for i := 0; i < len(filesMap); i++ {
		_, ok := filesMap[fmt.Sprintf("%06d", i+1)]
		if !ok {
			index = i
			break
		}
	}

	if index == -1 {
		index = len(filesMap) + 2
	}

	return fmt.Sprintf("%s_%06d%s", process, index, extension)
}

func getMemProFile(config *config.FluentConfig) string {
	proFile := *memproFile
	if proFile == "" {
		proFile = config.MemProfile
	}

	proFile = getRandomProfileName(proFile, "mprof")

	if proFile != "" {
		proFile = time.Now().Format(proFile)
	}
	return strings.TrimSpace(proFile)
}

func getCPUProFile(config *config.FluentConfig) string {
	proFile := *cpuproFile
	if proFile == "" {
		proFile = config.CPUProfile
	}

	proFile = getRandomProfileName(proFile, "cprof")

	if proFile != "" {
		proFile = time.Now().Format(proFile)
	}
	return strings.TrimSpace(proFile)
}

func getServiceMode(config *config.FluentConfig) (mode lib.ServiceMode, smode string, ok bool) {
	ok = true
	smode = strings.TrimSpace(*servicemode)
	if smode == "" {
		smode = strings.TrimSpace(config.ServiceMode)
	}

	smode = strings.ToLower(smode)

	mode = lib.SmInOut
	switch smode {
	case lib.In:
		mode = lib.SmIn
	case lib.Out:
		mode = lib.SmOut
	case lib.InOut:
		mode = lib.SmInOut
	default:
		ok = false
	}

	return
}

func startAdminModule(config *config.FluentConfig, logger log.Logger, quitSignal <-chan bool) {
	defer recover()

	if config.Admin.Enabled {
		params := make(map[string]interface{}, 5)

		params["addr"] = config.Admin.HTTPAddress
		params["certFile"] = config.Admin.TLS.CertFile
		params["keyFile"] = config.Admin.TLS.KeyFile
		params["caFile"] = config.Admin.TLS.CAFile
		params["verifySsl"] = config.Admin.TLS.VerifySsl

		server, _ := httpsrv.NewHttpServer(NewAdminRouter(), logger, params)
		server.Start(quitSignal)
	}
}
