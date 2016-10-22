package inout

import (
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/config"
	"github.com/ocdogan/fluentgo/disk"
	"github.com/ocdogan/fluentgo/log"
)

type bufferWatchDog struct {
	processing     int32
	checking       int32
	enabled        bool
	immediate      bool
	path           string
	onEverySec     time.Duration
	sizeInMB       int
	percentage     float64
	keepLastXHours int
	logger         log.Logger
	completed      chan bool
}

func newBufferWatchDog(config *config.FluentConfig, logger log.Logger) *bufferWatchDog {
	path := (&config.Inputs.Buffer).GetPath() + "completed" + string(os.PathSeparator)

	return &bufferWatchDog{
		path:           path,
		logger:         logger,
		enabled:        config.Inputs.Buffer.DiskWatchDog.Enabled,
		immediate:      config.Inputs.Buffer.DiskWatchDog.Immediate,
		sizeInMB:       (&config.Inputs.Buffer.DiskWatchDog).GetMinSizeInMB(),
		percentage:     (&config.Inputs.Buffer.DiskWatchDog).GetMinSizeInPercentage(),
		onEverySec:     (&config.Inputs.Buffer.DiskWatchDog).GetOnEverySec(),
		keepLastXHours: (&config.Inputs.Buffer.DiskWatchDog).GetKeepLastXHours(),
	}
}

func (b *bufferWatchDog) Close() {
	defer recover()

	if b.Processing() {
		c := b.completed
		if c != nil {
			defer func() {
				b.completed = nil
			}()
			close(c)
		}
	}
}

func (b *bufferWatchDog) Processing() bool {
	return atomic.LoadInt32(&b.processing) != 0
}

func (b *bufferWatchDog) Process() {
	if b.enabled && atomic.LoadInt32(&b.processing) == 0 {
		go b.run()
	}
}

func (b *bufferWatchDog) run() {
	if !b.enabled || !atomic.CompareAndSwapInt32(&b.processing, 0, 1) {
		return
	}

	completed := false
	completeSignal := b.completed

	defer func() {
		defer recover()

		recover()
		atomic.StoreInt32(&b.processing, 0)

		if b.logger != nil {
			b.logger.Println("Stopping 'BUFFERWATCHDOG' service...")
		}

		if !completed && completeSignal != nil {
			func() {
				defer recover()
				completeSignal <- true
			}()
		}
	}()

	if b.logger != nil {
		b.logger.Println("Starting 'BUFFERWATCHDOG' service...")
	}

	if b.immediate {
		go b.runDog()
	}

	count := 0
	for !completed && b.enabled {
		select {
		case <-completeSignal:
			completed = true
			return
		case <-time.After(b.onEverySec):
			if completed {
				return
			}

			if atomic.LoadInt32(&b.checking) == 0 {
				go b.runDog()
			}

			count++
			count = count % 10
			if count == 0 {
				time.Sleep(500 * time.Millisecond)
			}
		}
	}
}

func (b *bufferWatchDog) needsTrim() (trimSize uint64, result bool) {
	defer func() {
		if e := recover(); e != nil {
			trimSize = 0
			result = false
		}
	}()

	if b.percentage <= 0 && b.sizeInMB <= 0 {
		return 0, false
	}

	total, free, err := disk.Space(b.path)
	if err != nil {
		return 0, false
	}

	var trimBy1, trimBy2 float64

	if b.sizeInMB > 0 {
		trimBy1 = (float64(lib.MByte) * float64(b.sizeInMB)) - float64(free)
		trimBy1 = math.Ceil(lib.MaxFloat64(0, trimBy1))
	}

	if b.percentage > 0 {
		trimBy2 = (b.percentage * float64(total)) - float64(free)
		trimBy2 = math.Ceil(lib.MaxFloat64(0, trimBy2))
	}

	trimSize = uint64(lib.MaxFloat64(trimBy1, trimBy2))
	result = trimSize > 0
	return
}

func (b *bufferWatchDog) runDog() {
	if !atomic.CompareAndSwapInt32(&b.checking, 0, 1) {
		return
	}
	defer func() {
		recover()
		atomic.StoreInt32(&b.checking, 0)
	}()

	if exists, err := lib.PathExists(b.path); !exists || err != nil {
		return
	}

	var (
		trim     bool
		trimSize uint64
	)

	if trimSize, trim = b.needsTrim(); trim {
		searchFor := b.path + "*"

		matches, err := filepath.Glob(searchFor)
		if err != nil || len(matches) == 0 {
			return
		}

		count := 0
		var files = lib.FileInfoList(make([]*lib.FileInfo, 0, len(matches)))

		for _, filename := range matches {
			if !b.Processing() {
				return
			}

			count++
			count = count % 100
			if count == 0 {
				time.Sleep(time.Millisecond)
			}

			checkDate := false
			var timeTreshold time.Duration

			if b.keepLastXHours > 0 {
				checkDate = true
				timeTreshold = time.Duration(b.keepLastXHours) * time.Hour
			}

			fi, err := os.Stat(filename)
			if err == nil && !fi.IsDir() {
				date := fi.ModTime()
				if !checkDate || time.Now().Sub(date) >= timeTreshold {
					files = append(files,
						lib.NewFileInfo(filename, date, uint64(fi.Size())))
				}
			}
		}

		if len(files) > 1 {
			sort.Stable(files)
		}

		if len(files) == 0 {
			return
		}

		count = 0
		for _, file := range files {
			if !b.Processing() {
				return
			}

			count++
			count = count % 100
			if count == 0 {
				time.Sleep(500 * time.Millisecond)

				trimSize, trim = b.needsTrim()
				if !trim {
					return
				}
			}

			err := os.Remove(file.Name())
			if err == nil {
				trimSize -= file.Size()
				if trimSize <= 0 {
					return
				}
			}
		}
	}
}
