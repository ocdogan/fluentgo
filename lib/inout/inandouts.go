package inout

import (
	"sync/atomic"
	"time"

	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/lib/config"
	"github.com/ocdogan/fluentgo/lib/log"
)

type InAndOuts struct {
	state      uint32
	iman       *InManager
	oman       *OutManager
	mode       lib.ServiceMode
	logger     log.Logger
	quitSignal <-chan bool
}

func NewInOutManager(mode lib.ServiceMode, config *config.FluentConfig, logger log.Logger, quitSignal <-chan bool) *InAndOuts {
	var (
		iman *InManager
		oman *OutManager
	)

	if mode == lib.SmIn || mode == lib.SmInOut {
		iman = NewInManager(config, logger)
	}

	if mode == lib.SmOut || mode == lib.SmInOut {
		oman = NewOutManager(config, logger)
	}

	return &InAndOuts{
		iman:       iman,
		oman:       oman,
		mode:       mode,
		logger:     logger,
		quitSignal: quitSignal,
	}
}

func (iao *InAndOuts) Process() {
	if !atomic.CompareAndSwapUint32(&iao.state, 0, 1) {
		return
	}

	defer func() {
		atomic.StoreUint32(&iao.state, 0)
		iao.logger.Println("Stopping service...")
	}()
	iao.logger.Println("Starting service...")

	imActive := iao.iman != nil
	omActive := iao.oman != nil

	if !(imActive || omActive) {
		return
	}

	// Handle orphan files before async process start
	if imActive {
		iao.iman.HandleOrphans()
	}
	if omActive {
		iao.oman.HandleOrphans()
	}

	// Start processes
	var (
		imCompleted <-chan bool
		omCompleted <-chan bool
	)

	if imActive {
		imCompleted = iao.iman.Process()
	}
	if omActive {
		omCompleted = iao.oman.Process()
	}

	defer func() {
		if iao.iman != nil {
			iao.iman.Close()
		}

		if iao.oman != nil {
			iao.oman.Close()
		}
	}()

	for imActive || omActive {
		select {
		case <-iao.quitSignal:
			imActive = false
			omActive = false
			return
		case <-imCompleted:
			imActive = false
			if !omActive {
				return
			}
		case <-omCompleted:
			omActive = false
			if !imActive {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
}

func (iao *InAndOuts) GetInputs() []InOutInfo {
	if iao != nil && iao.iman != nil {
		return iao.iman.GetInputs()
	}
	return nil
}

func (iao *InAndOuts) GetOutputs() []InOutInfo {
	if iao != nil && iao.oman != nil {
		return iao.oman.GetOutputs()
	}
	return nil
}
