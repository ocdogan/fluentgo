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
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"encoding/json"

	"github.com/ocdogan/fluentgo/config"
	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/log"
)

type outFile struct {
	count int
	size  int
	file  *os.File
}

type fileRollOut struct {
	count        int
	size         int
	enabled      bool
	onEverySec   time.Duration
	lastRollTime time.Time
}

type indexer struct {
	index     int32
	indexTime time.Time
}

type fileOut struct {
	sync.Mutex
	outHandler
	outputDir string
	subPath   string
	prefix    string
	extension string
	multiLog  bool
	rolling   fileRollOut
	ofile     *outFile
	lg        log.Logger
	index     *indexer
	preparing int32
}

func init() {
	RegisterOut("file", newFileOut)
	RegisterOut("fileout", newFileOut)
}

func newIndexer() *indexer {
	now := time.Now()
	now = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, time.Local)

	return &indexer{
		indexTime: now,
	}
}

func (idx *indexer) next() (id int32, prefix string) {
	now := time.Now()
	now = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, time.Local)

	prefix = fmt.Sprintf("%d%02d%02dT%02d%02dx", now.Year(), now.Month(),
		now.Day(), now.Hour(), now.Minute())

	if now != idx.indexTime {
		id = 1
		atomic.StoreInt32(&idx.index, 1)
		idx.indexTime = now
	} else {
		id = atomic.AddInt32(&idx.index, 1)
		if id >= math.MaxInt32-1 {
			id = 1
			atomic.StoreInt32(&idx.index, 1)
		}
	}
	return
}

func newFileOut(manager InOutManager, params map[string]interface{}) OutSender {
	oh := newOutHandler(manager, params)
	if oh == nil {
		return nil
	}

	outputDir, ok := config.ParamAsString(params, "path")
	if !ok || outputDir == "" {
		return nil
	}
	outputDir = lib.PreparePath(outputDir)

	if exists, err := lib.PathExists(outputDir); !exists || err != nil {
		os.MkdirAll(outputDir, 0777)
	}

	prefix, ok := config.ParamAsString(params, "prefix")
	if ok && prefix != "" {
		prefix = strings.TrimSpace(prefix)
		if prefix != "" && prefix[len(prefix)-1] != '-' {
			prefix += "-"
		}
	}

	subPath, ok := config.ParamAsString(params, "subPath")
	if ok && subPath != "" {
		subPath = strings.TrimSpace(subPath)
	}

	extension, ok := config.ParamAsString(params, "extension")
	if !ok {
		extension = ".log"
	} else {
		if extension != "" {
			extension = strings.TrimSpace(extension)
		}

		if extension == "" {
			extension = ".log"
		} else if extension[0] != '.' {
			extension = "." + extension
		}
	}

	multiLog, _ := config.ParamAsBool(params, "multiLog")

	rollCount, ok := config.ParamAsInt(params, "roll.count")
	if ok {
		if rollCount < 0 {
			rollCount = 0
		} else if rollCount > 1000000 {
			rollCount = 1000000
		}
	}

	rollSize, ok := config.ParamAsInt(params, "roll.size")
	if ok {
		if rollSize < 0 {
			rollSize = 0
		} else if rollSize > 100*1024*1024 {
			rollSize = 100 * 1024 * 1024
		}
	}

	rollOnEverySec, ok := config.ParamAsDuration(params, "roll.onEverySec")
	if ok {
		rollOnEverySec = lib.MinDuration(600, lib.MaxDuration(0, rollOnEverySec)) * time.Second
	}

	fo := &fileOut{
		outHandler: *oh,
		outputDir:  outputDir,
		subPath:    subPath,
		prefix:     prefix,
		extension:  extension,
		multiLog:   multiLog,
		lg:         manager.GetLogger(),
		index:      newIndexer(),
		rolling: fileRollOut{
			count:        rollCount,
			size:         rollSize,
			onEverySec:   rollOnEverySec,
			lastRollTime: time.Now(),
			enabled:      multiLog && (rollCount > 0 || rollSize > 0),
		},
	}

	fo.iotype = "FILEOUT"

	fo.runFunc = fo.waitComplete
	fo.afterCloseFunc = fo.funcAfterClose
	fo.getDestinationFunc = fo.funcGetObjectName
	fo.sendChunkFunc = fo.funcPutMessages

	return fo
}

func (fo *fileOut) nextLogFile() string {
	start, prefix := fo.index.next()

	var (
		err     error
		exists  bool
		rest    string
		newName string
	)

	outpath := fo.outputDir
	if fo.subPath != "" {
		subPath := func(sp string) (result string) {
			result = sp
			defer func() {
				if err := recover(); err != nil {
					result = sp
				}
				if result != "" {
					result = strings.TrimSpace(result)
				}
			}()
			result = time.Now().Format(sp)
			return
		}(fo.subPath)

		if subPath != "" {
			outpath = lib.PreparePath(outpath + subPath)
		}
	}

	fileName := outpath + fo.prefix + prefix

	for i := start; i < math.MaxInt32; i++ {
		if fo.multiLog {
			rest = fmt.Sprintf("%04d", i) + fo.extension
		} else {
			rest = fmt.Sprintf("%06d", i) + fo.extension
		}

		newName, _ = filepath.Abs(fileName + rest)

		exists, err = lib.FileExists(newName)
		if !exists && err == nil {
			if exists, err := lib.PathExists(outpath); !exists || err != nil {
				os.MkdirAll(outpath, 0777)
			}
			return newName
		}
	}
	return ""
}

func (fo *fileOut) prepareFile(dataLen int) {
	if !atomic.CompareAndSwapInt32(&fo.preparing, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&fo.preparing, 0)

	ofile := fo.ofile
	dataLen = lib.MaxInt(0, dataLen)

	changeFile := ofile == nil || !fo.multiLog ||
		(fo.rolling.size > 0 && ofile.size+dataLen > fo.rolling.size) ||
		(fo.rolling.count > 0 && ofile.count+1 > fo.rolling.count) ||
		(time.Now().Sub(fo.rolling.lastRollTime) >= fo.rolling.onEverySec)

	if !changeFile {
		return
	}

	if fo.multiLog {
		defer func() {
			fo.rolling.lastRollTime = time.Now()
		}()
	}

	fo.ofile = nil
	if ofile != nil {
		defer func(file *os.File) {
			if file != nil {
				defer recover()
				file.Close()
			}
		}(ofile.file)
	}

	var file *os.File
	filename := fo.nextLogFile()

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

	fo.ofile = &outFile{
		file: file,
	}
}

func (fo *fileOut) funcAfterClose() {
}

func (fo *fileOut) funcGetObjectName() string {
	return "fileout"
}

func (fo *fileOut) writeToLog(msg ByteArray) {
	if len(msg) > 0 {
		defer recover()

		fo.Lock()
		defer fo.Unlock()

		data := []byte(msg)
		ln := len(data)

		if fo.multiLog {
			hasRet := false
			for _, rn := range msg {
				if rn == '\r' || rn == '\n' {
					hasRet = true
					break
				}
			}

			if hasRet {
				var jsonMsg map[string]interface{}

				err := json.Unmarshal(data, &jsonMsg)
				if err != nil {
					data, _ = json.Marshal(jsonMsg)
				}

				ln = len(data)
				if ln == 0 {
					return
				}
			}
		}

		fo.prepareFile(ln)

		ofile := fo.ofile

		if ofile != nil {
			f := ofile.file

			if f != nil {
				defer f.Sync()
				f.Write(data)

				var nln []byte
				if fo.multiLog {
					nln = lib.NewLine()
					f.Write(nln)
				}

				ofile.size += ln + len(nln)
				ofile.count++

				if ofile.count%100 == 0 {
					f.Sync()
				}
			}
		}
	}
}

func (fo *fileOut) funcPutMessages(messages []ByteArray, channel string) {
	if len(messages) == 0 {
		return
	}
	defer recover()

	for _, msg := range messages {
		if len(msg) > 0 {
			fo.writeToLog(msg)
		}
	}
}

func (fo *fileOut) Connect() error {
	return nil
}
