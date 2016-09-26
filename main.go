package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var (
	configPath  = flag.String("config", "", "The config.json file fully qualified path.")
	profileURL  = flag.String("profileURL", "", "Http endpoint serving profile data.")
	cpuproFile  = flag.String("cpuprofile", "", "write CPU profile to file")
	memproFile  = flag.String("memprofile", "", "write mem profile to file")
	servicemode = flag.String("servicemode", "", "defines how fluentgo service will act [pub|sub|pubsub]")
)

func main() {
	quitSignal := make(chan bool, 1)
	defer func() {
		defer recover()
		close(quitSignal)
	}()

	flag.Parse()

	listenQuitSignal(quitSignal)

	config := loadConfig(*configPath)
	logger := newLogger(&config.Log)

	mpFile := getMemProFile(config)
	cpFile := getCPUProFile(config)

	profURL := getProfileURL(config)
	if profURL != "" {
		go func() {
			defer recover()
			logger.Println(http.ListenAndServe(profURL, nil))
		}()
	}

	scheduleMemProfiler(mpFile, logger, quitSignal)

	fn := scheduleCPUProfiler(cpFile, logger)
	if fn != nil {
		defer func() {
			defer recover()
			fn()
		}()
	}

	smode, ok := getServiceMode(config)

	fmt.Printf("Service mode: %s\n", smode)
	if !ok {
		msg := fmt.Sprintf("Invalid service mode, setting service mode to 'inout'.")
		logger.Println(msg)
	}

	process(smode, config, logger, quitSignal)
}

func getProfileURL(config *fluentConfig) string {
	url := *profileURL
	if url == "" {
		url = config.ProfileURL
	}
	return strings.TrimSpace(url)
}

func getMemProFile(config *fluentConfig) string {
	proFile := *memproFile
	if proFile == "" {
		proFile = config.MemProfile
	}

	if proFile != "" {
		proFile = time.Now().Format(proFile)
	}
	return strings.TrimSpace(proFile)
}

func getCPUProFile(config *fluentConfig) string {
	proFile := *cpuproFile
	if proFile == "" {
		proFile = config.CPUProfile
	}

	if proFile != "" {
		proFile = time.Now().Format(proFile)
	}
	return strings.TrimSpace(proFile)
}

func process(smode string, config *fluentConfig, logger Logger, quitSignal <-chan bool) {
	logger.Println("Starting service...")
	defer logger.Println("Stopping service...")

	var (
		im *inManager
		om *outManager
	)

	if smode == in || smode == inout {
		im = newInManager(config, logger)
	}

	if smode == out || smode == inout {
		om = newOutManager(config, logger)
	}

	imActive := im != nil
	omActive := om != nil

	// Handle orphan files before async process start
	if imActive {
		im.HandleOrphans()
	}
	if omActive {
		om.HandleOrphans()
	}

	// Start processes
	var (
		imCompleted <-chan bool
		omCompleted <-chan bool
	)

	if imActive {
		imCompleted = im.Process()
	}
	if omActive {
		omCompleted = om.Process()
	}

	for imActive || omActive {
		select {
		case <-quitSignal:
			if imActive {
				imActive = false
				if im != nil {
					im.Close()
				}
			}

			if omActive {
				omActive = false
				if om != nil {
					om.Close()
				}
			}
		case <-imCompleted:
			imActive = false
		case <-omCompleted:
			omActive = false
		}

		time.Sleep(time.Millisecond)
	}
}

func getServiceMode(config *fluentConfig) (smode string, ok bool) {
	ok = true
	smode = strings.TrimSpace(*servicemode)
	if smode == "" {
		smode = strings.TrimSpace(config.ServiceMode)
	}

	smode = strings.ToLower(smode)

	if !(smode == in || smode == out || smode == inout) {
		smode = inout
		ok = false
	}
	return smode, ok
}

func listenQuitSignal(quitSignal chan<- bool) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)
	signal.Notify(sigc, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGQUIT)

	go func() {
		<-sigc
		fmt.Println("Termination signalled...")
		quitSignal <- true
	}()
}
