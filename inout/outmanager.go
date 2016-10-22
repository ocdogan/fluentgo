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

package inout

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/config"
	"github.com/ocdogan/fluentgo/log"
)

type OutSender interface {
	IOClient
	Send(messages []string)
}

type FuncNewOut func(manage InOutManager, params map[string]interface{}) OutSender

type OutManager struct {
	sync.Mutex
	dataPath          string
	dataPattern       string
	timestampKey      string
	timestampFormat   string
	bulkCount         int
	maxMessageSize    int
	flushOnEverySec   time.Duration
	sleepOnEverySec   time.Duration
	sleepForMSec      time.Duration
	lastFlushTime     time.Time
	processing        int32
	processingFiles   int32
	processingQueue   int32
	orphanDelete      bool
	orphanLastXDays   int
	outQ              *OutQueue
	logger            log.Logger
	outputs           map[lib.UUID]OutSender
	completed         chan bool
	completedChansMux sync.Mutex
	completedChans    []chan<- bool
}

var (
	outputMethods = make(map[string]FuncNewOut)
)

func RegisterOut(senderType string, fn FuncNewOut) {
	if fn != nil {
		senderType = strings.TrimSpace(senderType)
		if senderType != "" {
			if outputMethods == nil {
				outputMethods = make(map[string]FuncNewOut)
			}

			senderType = strings.ToLower(senderType)
			outputMethods[senderType] = fn
		}
	}
}

func UnregisterOut(senderType string) {
	if outputMethods != nil {
		senderType = strings.TrimSpace(senderType)
		if senderType != "" {
			senderType = strings.ToLower(senderType)
			if _, ok := outputMethods[senderType]; ok {
				delete(outputMethods, senderType)
			}
		}
	}
}

func NewOutManager(config *config.FluentConfig, logger log.Logger) *OutManager {
	if logger == nil {
		logger = log.NewDummyLogger()
	}

	dataPath := (&config.Outputs).GetDataPath(&config.Inputs)

	dataPattern := (&config.Outputs).GetDataPattern(&config.Inputs)
	dataPattern = dataPath + dataPattern

	manager := &OutManager{
		dataPath:        dataPath,
		dataPattern:     dataPattern,
		lastFlushTime:   time.Now(),
		logger:          logger,
		outputs:         make(map[lib.UUID]OutSender),
		timestampKey:    strings.TrimSpace(config.Outputs.TimestampKey),
		timestampFormat: (&config.Outputs).GetTimestampFormat(),
		bulkCount:       (&config.Outputs).GetBulkCount(),
		maxMessageSize:  (&config.Outputs).GetMaxMessageSize(),
		flushOnEverySec: (&config.Outputs).GetFlushOnEverySec(),
		sleepOnEverySec: (&config.Outputs).GetSleepOnEverySec(),
		sleepForMSec:    (&config.Outputs).GetSleepForMillisec(),
		orphanDelete:    (&config.Outputs.OrphanFiles).GetDeleteAll(),
		orphanLastXDays: (&config.Outputs.OrphanFiles).GetReplayLastXDays(),
		outQ:            NewOutQueue((&config.Outputs.Queue).GetParams()),
	}

	manager.setOutputs(&config.Outputs)

	return manager
}

func (m *OutManager) GetOutputs() []InOutInfo {
	if m != nil && len(m.outputs) > 0 {
		var outputs []InOutInfo

		for _, out := range m.outputs {
			outputs = append(outputs, InOutInfo{
				ID:          out.ID().String(),
				Name:        out.Name(),
				Description: out.Description(),
				IOType:      out.GetIOType(),
				Enabled:     out.Enabled(),
				Processing:  out.Processing(),
			})
		}
		return outputs
	}
	return nil
}

func (m *OutManager) FindInput(id string) IOClient {
	return nil
}

func (m *OutManager) FindOutput(id string) IOClient {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}

	uuid, err := lib.ParseUUID(id)
	if err != nil {
		return nil
	}

	return m.outputs[*uuid]
}

func (m *OutManager) GetInputs() []InOutInfo {
	return nil
}

func (m *OutManager) setOutputs(config *config.OutputsConfig) {
	if config != nil {
		outs := m.outputs
		if outs == nil {
			outs = make(map[lib.UUID]OutSender)
			m.outputs = outs
		}

		for _, o := range config.Consumers {
			t := strings.ToLower(o.Type)

			if fn, ok := outputMethods[t]; ok {
				params := o.GetParamsMap()
				out := fn(m, params)

				if out != nil {
					v := reflect.ValueOf(out)
					if v.Kind() != reflect.Ptr || !v.IsNil() {
						out.SetName(o.Name)
						out.SetDescription(o.Description)

						outs[out.ID()] = out
					}
				}
			}
		}
	}
}

func (m *OutManager) GetMaxMessageSize() int {
	return m.maxMessageSize
}

func (m *OutManager) GetInQueue() *InQueue {
	return nil
}

func (m *OutManager) GetOutQueue() *OutQueue {
	return m.outQ
}

func (m *OutManager) GetLogger() log.Logger {
	return m.logger
}

func (m *OutManager) Close() {
	defer recover()

	if m.Processing() {
		c := m.completed
		if c != nil {
			defer func() {
				m.completed = nil
			}()
			close(c)
		}
	}
}

func (m *OutManager) Processing() bool {
	return atomic.LoadInt32(&m.processing) != 0
}

func (m *OutManager) Process(signal chan<- bool) {
	if !atomic.CompareAndSwapInt32(&m.processing, 0, 1) {
		if signal != nil {
			signal <- true
		}
		return
	}

	if len(m.outputs) == 0 {
		atomic.StoreInt32(&m.processing, 0)
		if signal != nil {
			signal <- true
		}
		return
	}

	m.completed = make(chan bool)
	m.SignalOnComplete(signal)

	go m.feedOutputs()
}

func (m *OutManager) SignalOnComplete(signal chan<- bool) {
	if signal != nil {
		m.completedChansMux.Lock()
		defer m.completedChansMux.Unlock()

		m.completedChans = append(m.completedChans, signal)
	}
}

func (m *OutManager) RemoveCompleteSignal(signal chan<- bool) {
	if signal != nil {
		m.completedChansMux.Lock()
		defer m.completedChansMux.Unlock()

		if len(m.completedChans) > 0 {
			for i, ch := range m.completedChans {
				if ch == signal {
					m.completedChans = append(m.completedChans[:i], m.completedChans[i+1:]...)
					break
				}
			}
		}
	}
}

func (m *OutManager) signalCompleted() {
	if len(m.completedChans) > 0 {
		m.completedChansMux.Lock()
		defer m.completedChansMux.Unlock()

		for _, ch := range m.completedChans {
			if ch != nil {
				func(signal chan<- bool) {
					defer recover()
					ch <- true
				}(ch)
			}
		}
	}
}

func (m *OutManager) closeOutputs() {
	if len(m.outputs) > 0 {
		for _, out := range m.outputs {
			func() {
				defer recover()
				out.Close()
			}()
		}
	}
}

func (m *OutManager) feedOutputs() {
	allCompleted := false
	completed := m.completed

	defer func() {
		defer func() {
			recover()
			m.signalCompleted()
		}()

		recover()
		atomic.StoreInt32(&m.processing, 0)

		if m.logger != nil {
			m.logger.Println("Stopping 'OUT' manager...")
		}

		m.closeOutputs()

		if !allCompleted {
			func(cmp chan bool) {
				if cmp != nil {
					defer recover()
					cmp <- true
				}
			}(completed)
		}
	}()

	if m.logger != nil {
		m.logger.Println("Starting 'OUT' manager...")
	}

	if m.outputs != nil {
		for _, out := range m.outputs {
			if out.Enabled() {
				go out.Run()
			}
		}
	}

	filesProcessed := make(chan bool)
	go m.processFiles(filesProcessed)

	queueProcessed := make(chan bool)
	go m.processQueue(queueProcessed)

	select {
	case <-filesProcessed:
		if allCompleted {
			return
		}
		filesProcessed := make(chan bool)
		go m.processFiles(filesProcessed)
	case <-queueProcessed:
		return
	case <-completed:
		allCompleted = true
		return
	}
}

func (m *OutManager) processFiles(filesProcessed chan<- bool) {
	if atomic.CompareAndSwapInt32(&m.processingFiles, 0, 1) {
		defer func() {
			recover()
			atomic.StoreInt32(&m.processingFiles, 0)

			time.Sleep(100 * time.Millisecond)
			close(filesProcessed)
		}()

		if len(m.outputs) == 0 || m.dataPath == "" {
			return
		}

		fileErrors := make(map[string]struct{})

		for m.Processing() {
			if exists, _ := lib.PathExists(m.dataPath); !exists {
				time.Sleep(5 * time.Second)
				continue
			}

			filenames, err := filepath.Glob(m.dataPattern)
			if err != nil || len(filenames) == 0 {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			filenames = filenames[:lib.MinInt(10, len(filenames))]

			lastSleepTime := time.Now()

			for _, fname := range filenames {
				if !m.Processing() {
					return
				}

				if _, ok := fileErrors[fname]; !ok {
					if m.DoSleep(lastSleepTime) {
						lastSleepTime = time.Now()
					}

					m.processFile(fname)
					if ok, _ := lib.FileExists(fname); ok {
						fileErrors[fname] = struct{}{}
					}
				}
			}
		}
	}
}

func (m *OutManager) waitForPush() {
	i := 0
	// wait for push
	for m.Processing() &&
		m.outQ != nil && !m.outQ.CanPush() {

		i++
		time.Sleep(time.Duration(i) * time.Millisecond)
		if i >= 25 {
			i = 0
		}
	}
}

func (m *OutManager) pushToQueue(data string) {
	if m.outQ != nil && m.Processing() {
		m.waitForPush()
		m.outQ.Push(data)
	}
}

func (m *OutManager) processFile(filename string) {
	defer recover()

	m.waitForPush()

	f, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil || f == nil {
		f = nil
		return
	}

	defer func() {
		defer m.fileProcessed(filename)
		if f != nil {
			f.Close()
		}
	}()

	st, err := f.Stat()
	if err != nil {
		return
	}

	fsize := st.Size()
	if fsize < 4 {
		return
	}

	stamp := make([]byte, 4)

	// Check the FILE_END stamp
	n, err := f.ReadAt(stamp, fsize-4)
	if err != nil || n != 4 {
		return
	}

	f.Seek(0, 0)

	var (
		data        []byte
		ln          int
		hasData     bool
		start, stop uint32
	)

	for m.Processing() {
		// Read START stamp
		n, err = f.Read(stamp)
		if n == 0 || err != nil {
			break
		}

		start = binary.BigEndian.Uint32(stamp)
		if start != 0 {
			break
		}

		// Read data length
		n, err = f.Read(stamp)
		if n == 0 || err != nil {
			break
		}

		ln = int(binary.BigEndian.Uint32(stamp))
		if ln > lib.InvalidMessageSize {
			break
		}

		hasData = ln > 0

		// Read message data
		if hasData {
			if m.maxMessageSize > 0 && ln > m.maxMessageSize {
				_, err = f.Seek(int64(ln), 1)
				if err != nil {
					break
				}
			} else {
				if data == nil || ln > cap(data) {
					data = make([]byte, ln)
				} else {
					data = data[0:ln]
				}

				n, err = f.Read(data)
				if n != ln || err != nil {
					break
				}

				data = m.appendTimestamp(data)
			}

			hasData = len(data) > 0
		}

		// Read STOP stamp
		n, err = f.Read(stamp)
		if n == 0 || err != nil {
			break
		}

		stop = binary.BigEndian.Uint32(stamp)
		if stop != 0 {
			break
		}

		// Process message
		if hasData {
			m.pushToQueue(string(data))
		}

		if !m.Processing() {
			return
		}
	}

	return
}

func (m *OutManager) waitForPop() {
	// wait for push
	i := 0
	for m.Processing() &&
		m.outQ != nil && !m.outQ.CanPop() {

		i++
		time.Sleep(time.Duration(i) * time.Millisecond)
		if i >= 25 {
			break
		}
	}
}

func (m *OutManager) processQueue(queueProcessed chan<- bool) {
	if atomic.CompareAndSwapInt32(&m.processingQueue, 0, 1) {
		defer func() {
			recover()
			atomic.StoreInt32(&m.processingQueue, 0)

			time.Sleep(100 * time.Millisecond)
			close(queueProcessed)
		}()

		if len(m.outputs) == 0 {
			return
		}

		for m.Processing() {
			m.waitForPop()

			if m.outQ == nil {
				return
			}

			chunk, ok := m.outQ.Pop(true)
			if ok && len(chunk) > 0 {
				m.tryToSend(chunk)
			}
		}
	}
}

func (m *OutManager) tryToSend(messages []string) {
	defer recover()

	if m.Processing() && len(messages) > 0 {
		func() {
			wg := lib.WorkGroup{}
			defer wg.Wait()

			for _, out := range m.outputs {
				if out.Enabled() && m.Processing() {
					wg.Add(1)
					go m.send(out, messages, &wg)
				}
			}
		}()
	}
}

func (m *OutManager) send(to OutSender, messages []string, wg *lib.WorkGroup) {
	defer wg.Done()
	if to != nil && m.Processing() {
		to.Send(messages)
	}
}

func (m *OutManager) fileProcessed(filename string) {
	defer func() {
		if e := recover(); e != nil {
			defer recover()

			dir := filepath.Dir(filename)
			if dir[len(dir)-1] != byte(os.PathSeparator) {
				dir += string(os.PathSeparator)
			}

			dir += "processed" + string(os.PathSeparator)
			if exists, err := lib.PathExists(dir); !exists || err != nil {
				os.MkdirAll(dir, 0777)
			}

			os.Rename(filename, dir+filepath.Base(filename))
		}
	}()
	os.Remove(filename)
}

func (m *OutManager) DoSleep(lastSleepTime time.Time) bool {
	if time.Now().Sub(lastSleepTime) >= m.sleepOnEverySec {
		time.Sleep(m.sleepForMSec)
		return true
	}
	return false
}

func (m *OutManager) doOrphanAction(searchFor string, movePath string, remove bool) {
	defer recover()

	filenames, err := filepath.Glob(searchFor)
	if err != nil || len(filenames) == 0 {
		return
	}

	for _, filename := range filenames {
		func() {
			defer recover()

			if exists, err := lib.FileExists(filename); exists && err == nil {
				if remove {
					os.Remove(filename)
				} else {
					newName := movePath + filepath.Base(filename)
					os.Rename(filename, newName)
				}
			}
		}()
	}
}

func (m *OutManager) HandleOrphans() {
	defer recover()

	exists, err := lib.PathExists(m.dataPath)
	if !exists || err != nil {
		return
	}

	// Delete all files
	if m.orphanDelete {
		m.doOrphanAction(m.dataPattern, "", true)
	} else if m.orphanLastXDays > 0 {
		if exists, err := lib.PathExists(m.dataPath); !exists || err != nil {
			os.MkdirAll(m.dataPath, 0777)
		}

		tempPath := m.dataPath + "tmp" + string(os.PathSeparator)
		if exists, err := lib.PathExists(tempPath); !exists || err != nil {
			os.MkdirAll(tempPath, 0777)
		}

		today := time.Now()
		today = time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.Local)

		for i := 0; i < m.orphanLastXDays; i++ {
			func(today time.Time) {
				defer recover()

				t := today.Add(time.Duration(-i) * 24 * time.Hour)
				searchFor := fmt.Sprintf("%s*%d%02d%02dT*", m.dataPath,
					t.Year(), t.Month(), t.Day())

				m.doOrphanAction(searchFor, tempPath, false)
			}(today)
		}

		// Remove rest of the files
		m.doOrphanAction(m.dataPattern, "", true)
		// Move temp files back to output
		m.doOrphanAction(tempPath+"*.*", m.dataPath, false)
	}
}

func (m *OutManager) appendTimestamp(data []byte) []byte {
	if len(data) > 0 {
		timestampKey := m.timestampKey
		if timestampKey != "" {
			defer recover()

			var j interface{}
			err := json.Unmarshal(data, &j)

			if err == nil && j != nil {
				msg, ok := j.(map[string]interface{})
				if ok && msg != nil {
					_, ok := msg[timestampKey]
					if !ok {
						msg[timestampKey] = time.Now().Format(m.timestampFormat)

						stamped, err := json.Marshal(msg)
						if err == nil {
							return stamped
						}
					}
				}
			}
		}
	}
	return data
}
