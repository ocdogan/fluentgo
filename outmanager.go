package main

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultOutBulkCount = 50
)

type outSender interface {
	Run()
	Enabled() bool
	Processing() bool
	Send(messages []string)
	Close()
}

type outManager struct {
	sync.Mutex
	dataPath        string
	dataPattern     string
	timestampKey    string
	timestampFormat string
	bulkCount       int
	maxMessageSize  int
	flushOnEvery    time.Duration
	sleepForMSec    time.Duration
	sleepOnEvery    time.Duration
	lastSleepTime   time.Time
	lastFlushTime   time.Time
	logger          Logger
	outputs         []outSender
	processing      int32
	processingFiles int32
	completed       chan bool
}

func newOutManager(config *fluentConfig, logger Logger) *outManager {
	if logger == nil {
		logger = &log.Logger{}
	}

	bulkCount := config.Outputs.BulkCount
	if bulkCount < 1 {
		bulkCount = defaultOutBulkCount
	}
	bulkCount = minInt(500, bulkCount)

	maxMessageSize := maxInt(config.Outputs.MaxMessageSize, -1)

	flushOnEvery := time.Duration(minInt64(600, maxInt64(2, int64(config.Outputs.FlushOnEvery)))) * time.Second

	sleepOnEvery := time.Duration(minInt64(60000, maxInt64(1, int64(config.Outputs.SleepOnEvery)))) * time.Second
	sleepForMSec := time.Duration(minInt64(30000, maxInt64(1, int64(config.Outputs.SleepForMillisec)))) * time.Millisecond

	dataPath := getDataPath(config)

	dataPath, _ = filepath.Abs(dataPath)
	if dataPath[len(dataPath)-1] != os.PathSeparator {
		dataPath += string(os.PathSeparator)
	}

	dataPattern := strings.TrimSpace(config.Outputs.Pattern)
	if dataPattern == "" {
		var m *inManager
		dataPattern = m.getBufferFilenamePrefix(config) +
			"*" + m.getBufferFilenameExtension(config)
	}
	dataPattern = dataPath + dataPattern

	timestampKey := strings.TrimSpace(config.Outputs.TimestampKey)

	timestampFormat := ""
	if timestampKey != "" {
		timestampFormat = strings.TrimSpace(config.Outputs.TimestampFormat)
		if timestampFormat == "" {
			timestampFormat = ISO8601Time
		}
	}

	manager := &outManager{
		dataPath:        dataPath,
		dataPattern:     dataPattern,
		timestampKey:    timestampKey,
		timestampFormat: timestampFormat,
		bulkCount:       bulkCount,
		maxMessageSize:  maxMessageSize,
		flushOnEvery:    flushOnEvery,
		sleepForMSec:    sleepForMSec,
		sleepOnEvery:    sleepOnEvery,
		lastFlushTime:   time.Now(),
		lastSleepTime:   time.Now(),
		logger:          logger,
	}

	manager.setOutputs(&config.Outputs)

	return manager
}

func (m *outManager) GetMaxMessageSize() int {
	return m.maxMessageSize
}

func (m *outManager) GetQueue() *DataQueue {
	return nil
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
			go m.processOutputs()
		}
	}
	return m.completed
}

func getDataPath(config *fluentConfig) string {
	dataPath := filepath.Clean(strings.TrimSpace(config.Outputs.Path))
	if dataPath == "" || dataPath == "." {
		dataPath = "." + string(os.PathSeparator) +
			"buffer" + string(os.PathSeparator) +
			"completed" + string(os.PathSeparator)
	} else if dataPath[len(dataPath)-1] != os.PathSeparator {
		dataPath += string(os.PathSeparator)
	}
	return dataPath
}

func (m *outManager) setOutputs(config *outputsConfig) {
	var result []outSender
	if config != nil {
		for _, o := range config.Consumers {
			t := strings.ToLower(o.Type)
			if t == "elastic" || t == "elasticsearch" {
				out := newElasticOut(m, &o)
				if out != nil {
					result = append(result, out)
				}
			} else if t == "redis" || t == "redisout" {
				out := newRedisOut(m, &o)
				if out != nil {
					result = append(result, out)
				}
			} else if t == "rabbit" || t == "rabbitout" {
				out := newRabbitOut(m, &o)
				if out != nil {
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

func (m *outManager) send(sender outSender, messages []string, wg *WorkGroup) {
	defer wg.Done()
	if sender != nil {
		sender.Send(messages)
	}
}

func (m *outManager) trySend(messages []string) (ret []string) {
	defer func() {
		recover()
		m.DoSleep()
	}()

	ret = messages
	if ret != nil && m.Processing() {
		mlen := len(ret)
		if mlen > 0 {
			send := mlen == m.bulkCount ||
				time.Now().Sub(m.lastFlushTime) >= m.flushOnEvery

			if send && m.Processing() {
				m.lastFlushTime = time.Now()
				defer func() {
					recover()
					ret = nil
				}()

				wg := WorkGroup{}

				for _, out := range m.outputs {
					wg.Add(1)
					if m.Processing() && out.Enabled() {
						go m.send(out, messages, &wg)
					}
				}

				wg.Wait()
			}
		}
	}
	return ret
}

func (m *outManager) processOutputs() {
	completed := false
	completeSignal := m.completed

	defer func() {
		defer recover()

		recover()
		atomic.StoreInt32(&m.processing, 0)

		if m.logger != nil {
			m.logger.Println("Stopping 'OUT' manager...")
		}

		if m.outputs != nil {
			for _, out := range m.outputs {
				func() {
					defer recover()
					out.Close()
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
		m.logger.Println("Starting 'OUT' manager...")
	}

	if m.outputs != nil {
		for _, out := range m.outputs {
			if out.Enabled() {
				go out.Run()
			}
		}
	}

	for {
		select {
		case <-completeSignal:
			completed = true
			continue
		default:
			if !completed && m.Processing() &&
				atomic.CompareAndSwapInt32(&m.processingFiles, 0, 1) {
				go m.processFiles()
			}
		}

		if completed {
			return
		}
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

func (m *outManager) DoSleep() bool {
	t := time.Now()
	if t.Sub(m.lastSleepTime) >= m.sleepOnEvery {
		m.lastSleepTime = t
		time.Sleep(m.sleepForMSec)
		return true
	}
	return false
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

func (m *outManager) processFile(filename string, messages []string) (ret []string) {
	ret = messages
	defer func() {
		recover()
		m.DoSleep()
	}()

	f, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil || f == nil {
		f = nil
		return ret
	}

	defer func() {
		defer m.fileProcessed(filename)
		if f != nil {
			f.Close()
		}
	}()

	st, err := f.Stat()
	if err != nil {
		return ret
	}

	fsize := st.Size()
	if fsize < 4 {
		return ret
	}

	stamp := make([]byte, 4)

	// Check the FILE_END stamp
	n, err := f.ReadAt(stamp, fsize-4)
	if err != nil || n != 4 {
		return
	}

	f.Seek(0, 0)

	var (
		msg         []byte
		ln          int
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

		// Read message data
		if ln > 0 {
			if m.maxMessageSize > 0 && ln > m.maxMessageSize {
				_, err = f.Seek(int64(ln), 1)
				if err != nil {
					break
				}
			} else {
				if msg == nil || ln > cap(msg) {
					msg = make([]byte, ln)
				} else {
					msg = msg[0:ln]
				}

				n, err = f.Read(msg)
				if n != ln || err != nil {
					break
				}

				msg = m.appendTimestamp(msg)
			}
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
		if ln > 0 {
			ret = append(ret, string(msg))
		}

		if !m.Processing() {
			return ret
		}

		ret = m.trySend(ret)
	}

	return ret
}

func (m *outManager) processFiles() {
	defer func() {
		recover()
		atomic.StoreInt32(&m.processingFiles, 0)

		m.DoSleep()
	}()

	if len(m.outputs) == 0 || m.dataPath == "" {
		return
	}

	var messages []string
	fileErrors := make(map[string]bool)

	for m.Processing() {
		if exists, _ := pathExists(m.dataPath); !exists {
			time.Sleep(5 * time.Second)
			continue
		}

		filenames, err := filepath.Glob(m.dataPattern)
		if err != nil || len(filenames) == 0 {
			time.Sleep(time.Second)
			continue
		}

		filenames = filenames[:minInt(10, len(filenames))]

		for _, fname := range filenames {
			if !m.Processing() {
				return
			}

			messages = m.processFile(fname, messages)
			if ok, _ := fileExists(fname); ok {
				fileErrors[fname] = true
			}

			// If there is any message left, process the remaining messages
			messages = m.trySend(messages)
		}
	}
}
