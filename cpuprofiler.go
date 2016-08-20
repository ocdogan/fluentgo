package main

import (
	"log"
	"os"
	"runtime/pprof"
)

func scheduleCPUProfiler(cpuproFile string) func() {
	defer recover()

	if cpuproFile != "" {
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
