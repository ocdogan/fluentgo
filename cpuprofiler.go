package main

import (
	"log"
	"os"
	"runtime/pprof"
)

func scheduleCPUProfiler(cpuproFile string, logger Logger) func() {
	defer recover()

	if cpuproFile != "" {
		if logger != nil {
			logger.Printf("Profiling CPU to %s", cpuproFile)
		}

		f, err := os.Create(cpuproFile)
		if err != nil {
			log.Fatal(err)
		}

		pprof.StartCPUProfile(f)
		return func() {
			pprof.StopCPUProfile()
		}
	}
	return nil
}
