package main

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
)

type inProvider interface {
	ioClient
}

type bufferFile struct {
	count int
	size  int
	file  *os.File
}

type inManager struct {
	sync.Mutex
	processing      int32
	poppingQueue    int32
	indexer         int64
	inputDir        string
	outputDir       string
	prefix          string
	extension       string
	timestampKey    string
	timestampFormat string
	maxMessageSize  int
	flushSize       int
	flushCount      int
	flushOnEverySec time.Duration
	lastFlushTime   time.Time
	lastProcessTime time.Time
	orphanDelete    bool
	orphanLastXDays int
	inputs          []inProvider
	logger          Logger
	inQ             *inQueue
	bufFile         *bufferFile
	completed       chan bool
}

func newInManager(config *fluentConfig, logger Logger) *inManager {
	if logger == nil {
		logger = newDummyLogger()
	}

	inputDir := (&config.Inputs.Buffer).getPath()
	outputDir := inputDir + "completed" + string(os.PathSeparator)

	if exists, err := pathExists(outputDir); !exists || err != nil {
		os.MkdirAll(outputDir, 0777)
	}

	manager := &inManager{
		logger:          logger,
		indexer:         int64(-1),
		inputDir:        inputDir,
		outputDir:       outputDir,
		lastFlushTime:   time.Now(),
		lastProcessTime: time.Now(),
		prefix:          (&config.Inputs.Buffer).getPrefix(),
		extension:       (&config.Inputs.Buffer).getExtension(),
		flushSize:       (&config.Inputs.Buffer).getFlushSize(),
		flushCount:      (&config.Inputs.Buffer).getFlushCount(),
		flushOnEverySec: (&config.Inputs.Buffer).getFlushOnEverySec(),
		maxMessageSize:  (&config.Inputs.Buffer).getMaxMessageSize(),
		timestampFormat: (&config.Inputs.Buffer).getTimestampFormat(),
		timestampKey:    strings.TrimSpace(config.Inputs.Buffer.TimestampKey),
		orphanDelete:    (&config.Inputs.OrphanFiles).getDeleteAll(),
		orphanLastXDays: (&config.Inputs.OrphanFiles).getReplayLastXDays(),
		inQ:             newInQueue((&config.Inputs.Queue).getMaxParams()),
	}

	manager.setInputs(&config.Inputs)

	return manager
}

func (m *inManager) GetMaxMessageSize() int {
	return m.maxMessageSize
}

func (m *inManager) GetInQueue() *inQueue {
	return m.inQ
}

func (m *inManager) GetOutQueue() *outQueue {
	return nil
}

func (m *inManager) GetLogger() Logger {
	return m.logger
}

func (m *inManager) Close() {
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

func (m *inManager) DoSleep() bool {
	return false
}

func (m *inManager) Processing() bool {
	return atomic.LoadInt32(&m.processing) != 0
}

func (m *inManager) Process() (completed <-chan bool) {
	if atomic.CompareAndSwapInt32(&m.processing, 0, 1) {
		if len(m.inputs) > 0 {
			m.completed = make(chan bool)
			go m.processInputs()
		}
	}
	return m.completed
}

func (m *inManager) setInputs(config *inputsConfig) {
	var result []inProvider

	if config != nil {
		var in inProvider

		for _, p := range config.Producers {
			t := strings.ToLower(p.Type)

			in = nil
			if t == "redischan" || t == "redischanin" {
				in = newRedisChanIn(m, &p)
			} else if t == "redislist" || t == "redislistin" {
				in = newRedisListIn(m, &p)
			} else if t == "kinesis" || t == "kinesisin" {
				in = newKinesisIn(m, &p)
			} else if t == "sqs" || t == "sqsin" {
				in = newSqsIn(m, &p)
			} else if t == "rabbit" || t == "rabbitin" {
				in = newRabbitIn(m, &p)
			} else if t == "tcp" || t == "tcpin" {
				in = newTCPIn(m, &p)
			}

			if in != nil {
				v := reflect.ValueOf(in)
				if v.Kind() != reflect.Ptr || !v.IsNil() {
					result = append(result, in)
				}
			}
		}
	}

	if result != nil {
		m.inputs = result
	} else {
		m.inputs = make([]inProvider, 0)
	}
}

func (m *inManager) appendTimestamp(data []byte) []byte {
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

func (m *inManager) processQueue() {
	if !atomic.CompareAndSwapInt32(&m.poppingQueue, m.poppingQueue, int32(1)) {
		return
	}

	defer func() {
		defer atomic.StoreInt32(&m.poppingQueue, int32(0))
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

func (m *inManager) nextBufferFile() string {
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

		exists, err = fileExists(bufName1)
		if !exists && err == nil {
			bufName2, _ = filepath.Abs(movedName + rest)

			exists, err = fileExists(bufName2)
			if !exists && err == nil {
				return bufName1
			}
		}
	}
	return ""
}

func (m *inManager) prepareBuffer(dataLen int) {
	bf := m.bufFile
	dataLen = maxInt(0, dataLen)

	changeFile := bf == nil ||
		(m.flushSize > 0 && bf.size+dataLen > m.flushSize) ||
		(m.flushCount > 0 && bf.count+1 > m.flushCount) ||
		(m.lastFlushTime.Sub(time.Now()) >= m.flushOnEverySec)

	if !changeFile {
		return
	}

	defer func(manager *inManager) {
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

func (m *inManager) doOrphanAction(searchFor string, remove bool) {
	defer recover()

	filenames, err := filepath.Glob(searchFor)
	if err != nil || len(filenames) == 0 {
		return
	}

	for _, filename := range filenames {
		func() {
			defer recover()

			if exists, err := fileExists(filename); exists && err != nil {
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

func (m *inManager) HandleOrphans() {
	defer recover()

	exists, err := pathExists(m.inputDir)
	if !exists || err != nil {
		return
	}

	// Delete all files
	if m.orphanDelete {
		searchFor := fmt.Sprintf("%s%s*%s", m.inputDir, m.prefix, m.extension)
		m.doOrphanAction(searchFor, true)
	} else if m.orphanLastXDays != 0 {
		if exists, err := pathExists(m.outputDir); !exists || err != nil {
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

func (m *inManager) doFileCompleted(bf *bufferFile) {
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

			if exists, err := pathExists(m.outputDir); !exists || err != nil {
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

func (m *inManager) writeToBuffer(data []byte) {
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

	stamp := make([]byte, 4)

	binary.BigEndian.PutUint32(stamp, 0)
	f.Write(stamp)

	binary.BigEndian.PutUint32(stamp, uint32(ln))
	f.Write(stamp)

	f.Write(data)

	binary.BigEndian.PutUint32(stamp, 0)
	f.Write(stamp)

	fi.size += ln + 12
	fi.count++
}

func (m *inManager) processInputs() {
	completed := false
	completeSignal := m.completed

	defer func() {
		defer recover()

		recover()
		atomic.StoreInt32(&m.processing, 0)

		if m.logger != nil {
			m.logger.Println("Stopping 'IN' manager...")
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
		m.logger.Println("Starting 'IN' manager...")
	}

	if len(m.inputs) > 0 {
		for _, in := range m.inputs {
			if in.Enabled() {
				go in.Run()
			}
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
