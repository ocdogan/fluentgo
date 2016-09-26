package profiler

import (
	"os"
	"runtime/pprof"

	"github.com/ocdogan/fluentgo/lib/log"
)

func ScheduleCPUProfiler(cpuproFile string, logger log.Logger) func() {
	defer recover()

	if cpuproFile != "" {
		if logger != nil {
			logger.Printf("Profiling CPU to %s", cpuproFile)
		}

		f, err := os.Create(cpuproFile)
		if err != nil {
			logger.Fatal(err)
		}

		pprof.StartCPUProfile(f)
		return func() {
			pprof.StopCPUProfile()
		}
	}
	return nil
}
