package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	configPath  = flag.String("config", "", "The config.json file fully qualified path.")
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
	scheduleMemProfiler(*memproFile, quitSignal)

	fn := scheduleCPUProfiler(*cpuproFile)
	if fn != nil {
		defer func() {
			defer recover()
			fn()
		}()
	}

	config := loadConfig(*configPath)
	logger := newLogger(&config.Log)

	smode, ok := getServiceMode(config)

	fmt.Printf("Service mode: %s\n", smode)
	if !ok {
		msg := fmt.Sprintf("Invalid service mode, setting service mode to 'inout'.")
		logger.Println(msg)
	}

	logger.Println("Starting service...")

	var (
		im          *inManager
		om          *outManager
		imCompleted <-chan bool
		omCompleted <-chan bool
	)

	if smode == "in" || smode == "inout" {
		im = newInManager(config, logger)
		imCompleted = im.Process()
	}

	if smode == "out" || smode == "inout" {
		om = newOutManager(config, logger)
		omCompleted = om.Process()
	}

	imactive := im != nil
	omactive := om != nil

	for imactive || omactive {
		select {
		case <-quitSignal:
			if !imactive && im != nil {
				im.Close()
			}
			imactive = true

			if !omactive && om != nil {
				om.Close()
			}
			omactive = true
		case <-imCompleted:
			imactive = true
		case <-omCompleted:
			omactive = true
		}
	}

	logger.Println("Stopping service...")
}

func getServiceMode(config *fluentConfig) (smode string, ok bool) {
	ok = true
	smode = strings.TrimSpace(*servicemode)
	if smode == "" {
		smode = strings.TrimSpace(config.ServiceMode)
	}

	smode = strings.ToLower(smode)

	if !(smode == "in" || smode == "out") {
		smode = "inout"
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
