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
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ocdogan/fluentgo/config"
	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/log"
)

type InProvider interface {
	IOClient
}

type FuncNewIn func(manage InOutManager, params map[string]interface{}) InProvider

type bufferFile struct {
	count int
	size  int
	file  *os.File
}

type InManager struct {
	sync.Mutex
	processing        int32
	poppingQueue      int32
	indexer           int64
	inputDir          string
	outputDir         string
	prefix            string
	extension         string
	timestampKey      string
	timestampFormat   string
	maxMessageSize    int
	flushSize         int
	flushCount        int
	flushOnEverySec   time.Duration
	lastFlushTime     time.Time
	lastProcessTime   time.Time
	orphanDelete      bool
	orphanLastXDays   int
	inputs            map[lib.UUID]InProvider
	logger            log.Logger
	inQ               *InQueue
	bufFile           *bufferFile
	watchDog          *bufferWatchDog
	completedChansMux sync.Mutex
	completedChans    []chan<- bool
	completed         chan bool
	preparing         int32
}

var (
	inputMethods = make(map[string]FuncNewIn)
)

func RegisterIn(providerType string, fn FuncNewIn) {
	if fn != nil {
		providerType = strings.TrimSpace(providerType)
		if providerType != "" {
			if inputMethods == nil {
				inputMethods = make(map[string]FuncNewIn)
			}

			providerType = strings.ToLower(providerType)
			inputMethods[providerType] = fn
		}
	}
}

func UnregisterIn(providerType string) {
	if inputMethods != nil {
		providerType = strings.TrimSpace(providerType)
		if providerType != "" {
			providerType = strings.ToLower(providerType)
			if _, ok := inputMethods[providerType]; ok {
				delete(inputMethods, providerType)
			}
		}
	}
}

func NewInManager(config *config.FluentConfig, logger log.Logger) *InManager {
	if logger == nil {
		logger = log.NewDummyLogger()
	}

	inputDir := (&config.Inputs.Buffer).GetPath()
	outputDir := inputDir + "completed" + string(os.PathSeparator)

	if exists, err := lib.PathExists(outputDir); !exists || err != nil {
		os.MkdirAll(outputDir, 0777)
	}

	manager := &InManager{
		logger:          logger,
		indexer:         int64(-1),
		inputDir:        inputDir,
		outputDir:       outputDir,
		lastFlushTime:   time.Now(),
		lastProcessTime: time.Now(),
		inputs:          make(map[lib.UUID]InProvider),
		prefix:          (&config.Inputs.Buffer).GetPrefix(),
		extension:       (&config.Inputs.Buffer).GetExtension(),
		flushSize:       (&config.Inputs.Buffer).GetFlushSize(),
		flushCount:      (&config.Inputs.Buffer).GetFlushCount(),
		flushOnEverySec: (&config.Inputs.Buffer).GetFlushOnEverySec(),
		maxMessageSize:  (&config.Inputs.Buffer).GetMaxMessageSize(),
		timestampFormat: (&config.Inputs.Buffer).GetTimestampFormat(),
		timestampKey:    strings.TrimSpace(config.Inputs.Buffer.TimestampKey),
		orphanDelete:    (&config.Inputs.OrphanFiles).GetDeleteAll(),
		orphanLastXDays: (&config.Inputs.OrphanFiles).GetReplayLastXDays(),
		inQ:             NewInQueue((&config.Inputs.Queue).GetMaxParams()),
		watchDog:        newBufferWatchDog(config, logger),
	}

	manager.setInputs(&config.Inputs)

	return manager
}

func (m *InManager) FindInput(id string) IOClient {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}

	uuid, err := lib.ParseUUID(id)
	if err != nil {
		return nil
	}

	return m.inputs[*uuid]
}

func (m *InManager) FindOutput(id string) IOClient {
	return nil
}

func (m *InManager) GetInputs() []InOutInfo {
	if m != nil && len(m.inputs) > 0 {
		var inputs []InOutInfo

		for _, in := range m.inputs {
			inputs = append(inputs, InOutInfo{
				ID:          in.ID().String(),
				Name:        in.Name(),
				Description: in.Description(),
				IOType:      in.GetIOType(),
				Enabled:     in.Enabled(),
				Processing:  in.Processing(),
			})
		}
		return inputs
	}
	return nil
}

func (m *InManager) GetInputsWithType(typ string) []InOutInfo {
	if m != nil && len(m.inputs) > 0 {
		typ = strings.TrimSpace(typ)
		if len(typ) > 0 {
			typ = strings.ToUpper(typ)

			var (
				itype  string
				inputs []InOutInfo
			)

			for _, in := range m.inputs {
				itype = strings.ToUpper(in.GetIOType())
				if typ == itype {
					inputs = append(inputs, InOutInfo{
						ID:          in.ID().String(),
						Name:        in.Name(),
						Description: in.Description(),
						IOType:      itype,
						Enabled:     in.Enabled(),
						Processing:  in.Processing(),
					})
				}
			}
			return inputs
		}
	}
	return nil
}

func (m *InManager) GetOutputs() []InOutInfo {
	return nil
}

func (m *InManager) GetMaxMessageSize() int {
	return m.maxMessageSize
}

func (m *InManager) GetInQueue() *InQueue {
	return m.inQ
}

func (m *InManager) GetOutQueue() *OutQueue {
	return nil
}

func (m *InManager) GetLogger() log.Logger {
	return m.logger
}

func (m *InManager) Close() {
	defer func() {
		recover()
		m.watchDog.Close()
	}()

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

func (m *InManager) DoSleep() bool {
	return false
}

func (m *InManager) Processing() bool {
	return atomic.LoadInt32(&m.processing) != 0
}

func (m *InManager) Process(signal chan<- bool) {
	if !atomic.CompareAndSwapInt32(&m.processing, 0, 1) {
		if signal != nil {
			signal <- true
		}
		return
	}

	if len(m.inputs) == 0 {
		atomic.StoreInt32(&m.processing, 0)
		if signal != nil {
			signal <- true
		}
		return
	}

	m.completed = make(chan bool)
	m.SignalOnComplete(signal)
	m.watchDog.Process()

	go m.processInputs()
}

func (m *InManager) SignalOnComplete(signal chan<- bool) {
	if signal != nil {
		m.completedChansMux.Lock()
		defer m.completedChansMux.Unlock()

		m.completedChans = append(m.completedChans, signal)
	}
}

func (m *InManager) RemoveCompleteSignal(signal chan<- bool) {
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

func (m *InManager) signalCompleted() {
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

func (m *InManager) setInputs(config *config.InputsConfig) {
	if config != nil {
		ins := m.inputs
		if ins == nil {
			ins = make(map[lib.UUID]InProvider)
			m.inputs = ins
		}

		for _, p := range config.Producers {
			t := strings.ToLower(p.Type)

			if fn, ok := inputMethods[t]; ok {
				params := p.GetParamsMap()
				in := fn(m, params)

				if in != nil {
					v := reflect.ValueOf(in)
					if v.Kind() != reflect.Ptr || !v.IsNil() {
						in.SetName(p.Name)
						in.SetDescription(p.Description)

						ins[in.ID()] = in
					}
				}
			}
		}
	}
}

func (m *InManager) appendTimestamp(data []byte) []byte {
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

func (m *InManager) processQueue() {
	if !atomic.CompareAndSwapInt32(&m.poppingQueue, 0, 1) {
		return
	}

	defer func() {
		defer atomic.StoreInt32(&m.poppingQueue, 0)
		recover()
	}()
	m.lastProcessTime = time.Now()

	var (
		data []byte
		ok   bool
	)

	var ln int

	for m.Processing() && m.inQ.Count() > 0 {
		data, ok = m.inQ.Pop()
		if !ok {
			break
		}

		ln = len(data)
		if ln > 0 && (m.maxMessageSize < 1 || ln <= m.maxMessageSize) {
			data = m.appendTimestamp(data)
			m.writeToBuffer(data)
		}
	}
}

func (m *InManager) nextBufferFile() string {
	t := time.Now()
	prefix := m.prefix + fmt.Sprintf("%d%02d%02dT%02dx", t.Year(), t.Month(), t.Day(), t.Hour())

	var (
		exists   bool
		err      error
		rest     string
		bufName1 string
		bufName2 string
	)

	fileName := m.inputDir + prefix
	movedName := m.outputDir + prefix

	start := atomic.AddInt64(&m.indexer, int64(1))
	if start == math.MaxInt64 {
		start = 0
		atomic.StoreInt64(&m.indexer, 0)
	}

	for i := start; i < math.MaxInt64; i++ {
		atomic.StoreInt64(&m.indexer, i)

		rest = fmt.Sprintf("%06d", i) + m.extension

		bufName1, _ = filepath.Abs(fileName + rest)

		exists, err = lib.FileExists(bufName1)
		if !exists && err == nil {
			bufName2, _ = filepath.Abs(movedName + rest)

			exists, err = lib.FileExists(bufName2)
			if !exists && err == nil {
				return bufName1
			}
		}
	}
	return ""
}

func (m *InManager) cycleBuffer() {
	m.prepareBuffer(0)
}

func (m *InManager) prepareBuffer(dataLen int) {
	if !atomic.CompareAndSwapInt32(&m.preparing, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&m.preparing, 0)

	bf := m.bufFile
	dataLen = lib.MaxInt(0, dataLen)

	changeFile := bf == nil ||
		(m.flushSize > 0 && bf.size+dataLen > m.flushSize) ||
		(m.flushCount > 0 && bf.count+1 > m.flushCount) ||
		(time.Now().Sub(m.lastFlushTime) >= m.flushOnEverySec)

	if !changeFile {
		return
	}

	defer func(manager *InManager) {
		manager.lastFlushTime = time.Now()
	}(m)

	m.bufFile = nil
	m.doFileCompleted(bf)

	var file *os.File
	filename := m.nextBufferFile()

	if filename != "" {
		var err error
		file, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		if err != nil {
			if file != nil {
				f := file
				file = nil

				f.Close()
			}
		}
	}

	m.bufFile = &bufferFile{
		file: file,
	}
}

func (m *InManager) doOrphanAction(searchFor string, remove bool) {
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
					newName := m.outputDir + filepath.Base(filename)
					os.Rename(filename, newName)
				}
			}
		}()
	}
}

func (m *InManager) HandleOrphans() {
	defer recover()

	exists, err := lib.PathExists(m.inputDir)
	if !exists || err != nil {
		return
	}

	// Delete all files
	if m.orphanDelete {
		searchFor := fmt.Sprintf("%s%s*%s", m.inputDir, m.prefix, m.extension)
		m.doOrphanAction(searchFor, true)
	} else if m.orphanLastXDays != 0 {
		if exists, err := lib.PathExists(m.outputDir); !exists || err != nil {
			os.MkdirAll(m.outputDir, 0777)
		}

		// Move all files to output directory
		if m.orphanLastXDays < 0 {
			searchFor := fmt.Sprintf("%s%s*%s", m.inputDir, m.prefix, m.extension)
			m.doOrphanAction(searchFor, false)
		} else {
			today := time.Now()
			today = time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.Local)

			for i := 0; i < m.orphanLastXDays; i++ {
				func(today time.Time) {
					defer recover()

					t := today.Add(time.Duration(-i) * 24 * time.Hour)
					searchFor := fmt.Sprintf("%s%s%d%02d%02dT*%s", m.inputDir, m.prefix,
						t.Year(), t.Month(), t.Day(), m.extension)

					m.doOrphanAction(searchFor, false)
				}(today)
			}

			// Remove rest of the files
			searchFor := fmt.Sprintf("%s%s*%s", m.inputDir, m.prefix, m.extension)
			m.doOrphanAction(searchFor, true)
		}
	}
}

func (m *InManager) doFileCompleted(bf *bufferFile) {
	defer recover()

	if bf == nil {
		return
	}

	file := bf.file
	if file == nil {
		return
	}
	bf.file = nil

	defer func() {
		defer func(filename string) {
			defer recover()

			if exists, err := lib.PathExists(m.outputDir); !exists || err != nil {
				os.MkdirAll(m.outputDir, 0777)
			}

			newName := m.outputDir + filepath.Base(filename)
			os.Rename(filename, newName)
		}(file.Name())

		file.Close()
	}()

	lnBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lnBytes, math.MaxUint32)
	file.Write(lnBytes)
}

func (m *InManager) writeToBuffer(data []byte) {
	ln := len(data)
	if ln == 0 {
		return
	}

	defer recover()

	m.Lock()
	defer m.Unlock()

	m.prepareBuffer(ln)

	fi := m.bufFile
	if fi == nil {
		return
	}

	f := fi.file
	if f == nil {
		return
	}

	// Record start
	f.Write(lib.RecStartBytes())

	// Record lebgth
	stamp := make([]byte, 4)
	binary.BigEndian.PutUint32(stamp, uint32(ln))
	f.Write(stamp)

	// Record
	f.Write(data)

	// Record stop
	f.Write(lib.RecStopBytes())

	fi.size += ln + 12
	fi.count++
}

func (m *InManager) processInputs() {
	completed := false
	completeSignal := m.completed

	defer func() {
		defer func() {
			recover()
			m.signalCompleted()
		}()

		recover()
		atomic.StoreInt32(&m.processing, 0)

		if m.logger != nil {
			m.logger.Println("* Stopping 'IN' manager...")
		}

		if m.inputs != nil {
			for _, in := range m.inputs {
				func() {
					defer recover()
					in.Close()
				}()
			}
		}

		if !completed && completeSignal != nil {
			func() {
				defer recover()
				completeSignal <- true
			}()
		}
	}()

	if m.logger != nil {
		m.logger.Println("* Starting 'IN' manager...")
	}

	if len(m.inputs) > 0 {
		for _, in := range m.inputs {
			if in.Enabled() {
				go in.Run()
			}
		}
	}

	if m.flushOnEverySec > 0 {
		ticker := time.NewTicker(m.flushOnEverySec)
		if ticker != nil {
			defer ticker.Stop()

			go func(manager *InManager, ticker *time.Ticker) {
				var cycling int32
				flushSleep := lib.MaxDuration(manager.flushOnEverySec-time.Second, 1)

				for range ticker.C {
					if !manager.Processing() {
						break
					}

					if atomic.CompareAndSwapInt32(&cycling, 0, 1) {
						defer atomic.StoreInt32(&cycling, 0)
						manager.cycleBuffer()
						if flushSleep > 0 {
							time.Sleep(flushSleep)
						}
					}
				}
			}(m, ticker)
		}
	}

	for !completed {
		select {
		case <-completeSignal:
			completed = true
			return
		case <-m.inQ.Ready():
			if completed {
				return
			}
			if atomic.LoadInt32(&m.poppingQueue) == 0 {
				t := time.Now()
				if t.Sub(m.lastProcessTime) >= time.Second {
					time.Sleep(time.Millisecond)
					go m.processQueue()
				}
			}
		}
	}

}
