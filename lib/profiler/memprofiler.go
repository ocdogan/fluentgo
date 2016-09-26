package profiler

import (
	"os"
	"runtime/pprof"
	"time"

	"github.com/ocdogan/fluentgo/lib/log"
)

func ScheduleMemProfiler(memproFile string, logger log.Logger, quit <-chan bool) {
	defer recover()

	if memproFile != "" {
		if logger != nil {
			logger.Printf("Profiling memory to %s", memproFile)
		}

		ticker := time.NewTicker(1 * time.Second)
		go func() {
			defer recover()

			f, err := os.Create(memproFile)
			if err != nil {
				logger.Fatal(err)
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
