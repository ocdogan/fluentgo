package main

import (
	"log"
	"os"
	"runtime/pprof"
	"time"
)

func scheduleMemProfiler(memproFile string, quit <-chan bool) {
	defer recover()

	if memproFile != "" {
		ticker := time.NewTicker(1 * time.Second)
		go func() {
			defer recover()

			f, err := os.Create(memproFile)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()

			for {
				select {
				case <-ticker.C:
					pprof.WriteHeapProfile(f)
				case <-quit:
					ticker.Stop()
					return
				}
			}
		}()
	}
}
