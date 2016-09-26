package main

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
)

type outSender interface {
	ioClient
	Send(messages []string)
}

type outManager struct {
	sync.Mutex
	dataPath        string
	dataPattern     string
	timestampKey    string
	timestampFormat string
	bulkCount       int
	maxMessageSize  int
	flushOnEverySec time.Duration
	sleepOnEverySec time.Duration
	sleepForMSec    time.Duration
	lastFlushTime   time.Time
	processing      int32
	processingFiles int32
	processingQueue int32
	orphanDelete    bool
	orphanLastXDays int
	outQ            *outQueue
	logger          Logger
	outputs         []outSender
	completed       chan bool
}

func newOutManager(config *fluentConfig, logger Logger) *outManager {
	if logger == nil {
		logger = newDummyLogger()
	}

	dataPath := (&config.Outputs).getDataPath(&config.Inputs)

	dataPattern := (&config.Outputs).getDataPattern(&config.Inputs)
	dataPattern = dataPath + dataPattern

	manager := &outManager{
		dataPath:        dataPath,
		dataPattern:     dataPattern,
		lastFlushTime:   time.Now(),
		logger:          logger,
		timestampKey:    strings.TrimSpace(config.Outputs.TimestampKey),
		timestampFormat: (&config.Outputs).getTimestampFormat(),
		bulkCount:       (&config.Outputs).getBulkCount(),
		maxMessageSize:  (&config.Outputs).getMaxMessageSize(),
		flushOnEverySec: (&config.Outputs).getFlushOnEverySec(),
		sleepOnEverySec: (&config.Outputs).getSleepOnEverySec(),
		sleepForMSec:    (&config.Outputs).getSleepForMillisec(),
		orphanDelete:    (&config.Outputs.OrphanFiles).getDeleteAll(),
		orphanLastXDays: (&config.Outputs.OrphanFiles).getReplayLastXDays(),
		outQ:            newOutQueue((&config.Outputs.Queue).getParams()),
	}

	manager.setOutputs(&config.Outputs)

	return manager
}

func (m *outManager) setOutputs(config *outputsConfig) {
	var result []outSender

	if config != nil {
		var out outSender

		for _, o := range config.Consumers {
			t := strings.ToLower(o.Type)

			out = nil
			if t == "elastic" || t == "elasticsearch" {
				out = newElasticOut(m, &o)
			} else if t == "redis" || t == "redisout" {
				out = newRedisOut(m, &o)
			} else if t == "s3" || t == "s3out" {
				out = newS3Out(m, &o)
			} else if t == "sqs" || t == "sqsout" {
				out = newSqsOut(m, &o)
			} else if t == "kinesis" || t == "kinesisout" {
				out = newKinesisOut(m, &o)
			} else if t == "rabbit" || t == "rabbitout" {
				out = newRabbitOut(m, &o)
			} else if t == "std" || t == "stdout" {
				out = newStdOut(m, &o)
			} else if t == "tcp" || t == "tcpout" {
				out = newTCPOut(m, &o)
			} else if t == "null" || t == "nullout" {
				out = newNullOut(m, &o)
			}

			if out != nil {
				v := reflect.ValueOf(out)
				if v.Kind() != reflect.Ptr || !v.IsNil() {
					result = append(result, out)
				}
			}
		}
	}

	if result != nil {
		m.outputs = result
	} else {
		m.outputs = make([]outSender, 0)
	}
}

func (m *outManager) GetMaxMessageSize() int {
	return m.maxMessageSize
}

func (m *outManager) GetInQueue() *inQueue {
	return nil
}

func (m *outManager) GetOutQueue() *outQueue {
	return m.outQ
}

func (m *outManager) GetLogger() Logger {
	return m.logger
}

func (m *outManager) Close() {
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

func (m *outManager) Processing() bool {
	return atomic.LoadInt32(&m.processing) != 0
}

func (m *outManager) Process() (completed <-chan bool) {
	if atomic.CompareAndSwapInt32(&m.processing, 0, 1) {
		if len(m.outputs) > 0 {
			m.completed = make(chan bool)
			go m.feedOutputs()
		}
	}
	return m.completed
}

func (m *outManager) closeOutputs() {
	if len(m.outputs) > 0 {
		for _, out := range m.outputs {
			func() {
				defer recover()
				out.Close()
			}()
		}
	}
}

func (m *outManager) feedOutputs() {
	allCompleted := false
	completed := m.completed

	defer func() {
		defer recover()

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

func (m *outManager) processFiles(filesProcessed chan<- bool) {
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
			if exists, _ := pathExists(m.dataPath); !exists {
				time.Sleep(5 * time.Second)
				continue
			}

			filenames, err := filepath.Glob(m.dataPattern)
			if err != nil || len(filenames) == 0 {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			filenames = filenames[:minInt(10, len(filenames))]

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
					if ok, _ := fileExists(fname); ok {
						fileErrors[fname] = struct{}{}
					}
				}
			}
		}
	}
}

func (m *outManager) waitForPush() {
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

func (m *outManager) pushToQueue(data string) {
	if m.outQ != nil && m.Processing() {
		m.waitForPush()
		m.outQ.Push(data)
	}
}

func (m *outManager) processFile(filename string) {
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
		if ln > InvalidMessageSize {
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

func (m *outManager) waitForPop() {
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

func (m *outManager) processQueue(queueProcessed chan<- bool) {
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

func (m *outManager) tryToSend(messages []string) {
	defer recover()

	if m.Processing() && len(messages) > 0 {
		func() {
			wg := WorkGroup{}
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

func (m *outManager) send(to outSender, messages []string, wg *WorkGroup) {
	defer wg.Done()
	if to != nil && m.Processing() {
		to.Send(messages)
	}
}

func (m *outManager) fileProcessed(filename string) {
	defer func() {
		if e := recover(); e != nil {
			defer recover()

			dir := filepath.Dir(filename)
			if dir[len(dir)-1] != byte(os.PathSeparator) {
				dir += string(os.PathSeparator)
			}

			dir += "processed" + string(os.PathSeparator)
			if exists, err := pathExists(dir); !exists || err != nil {
				os.MkdirAll(dir, 0777)
			}

			os.Rename(filename, dir+filepath.Base(filename))
		}
	}()
	os.Remove(filename)
}

func (m *outManager) DoSleep(lastSleepTime time.Time) bool {
	if time.Now().Sub(lastSleepTime) >= m.sleepOnEverySec {
		time.Sleep(m.sleepForMSec)
		return true
	}
	return false
}

func (m *outManager) doOrphanAction(searchFor string, movePath string, remove bool) {
	defer recover()

	filenames, err := filepath.Glob(searchFor)
	if err != nil || len(filenames) == 0 {
		return
	}

	for _, filename := range filenames {
		func() {
			defer recover()

			if exists, err := fileExists(filename); exists && err == nil {
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

func (m *outManager) HandleOrphans() {
	defer recover()

	exists, err := pathExists(m.dataPath)
	if !exists || err != nil {
		return
	}

	// Delete all files
	if m.orphanDelete {
		m.doOrphanAction(m.dataPattern, "", true)
	} else if m.orphanLastXDays > 0 {
		if exists, err := pathExists(m.dataPath); !exists || err != nil {
			os.MkdirAll(m.dataPath, 0777)
		}

		tempPath := m.dataPath + "tmp" + string(os.PathSeparator)
		if exists, err := pathExists(tempPath); !exists || err != nil {
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

func (m *outManager) appendTimestamp(data []byte) []byte {
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
