package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	minLogSize = 2 * 1024
	maxLogSize = 10 * 1024 * 1024
)

var (
	newline = []byte("\n")
)

type Logger interface {
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
	Fatalln(v ...interface{})

	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
	Panicln(v ...interface{})

	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

type logger struct {
	sync.Mutex
	enabled     bool
	path        string
	fileName    string
	rollingSize int
	size        int
	console     bool
	lgType      logType
}

func init() {
	if runtime.GOOS == "windows" {
		newline = []byte("\r\n")
	}
}

func newLogger(config *logConfig) *logger {
	lt := logAppend
	if config.Type == "rolling" {
		lt = logRolling
	}

	logPath := filepath.Clean(strings.TrimSpace(config.Path))
	if logPath == "" || logPath == "." {
		logPath = "." + string(os.PathSeparator) + "log" + string(os.PathSeparator)
	}
	logPath, _ = filepath.Abs(logPath)

	if logPath[len(logPath)-1] != os.PathSeparator {
		logPath += string(os.PathSeparator)
	}

	if ok, err := pathExists(logPath); !ok || err != nil {
		os.MkdirAll(logPath, 0777)
	}

	return &logger{
		enabled:     config.Enabled,
		lgType:      lt,
		path:        logPath,
		rollingSize: getRollingSize(config),
		console:     config.Console,
	}
}

func getRollingSize(config *logConfig) int {
	sz := config.RollingSize
	if sz < minLogSize {
		sz = minLogSize
	} else if sz > maxLogSize {
		sz = maxLogSize
	}
	return sz
}

func (l *logger) getFilename() string {
	if l == nil {
		return ""
	}

	l.Lock()
	defer l.Unlock()

	t := time.Now()

	prefix := ""
	if l.lgType == logAppend {
		prefix = fmt.Sprintf("%d%02d%02d", t.Year(), t.Month(), t.Day())
	} else {
		prefix = fmt.Sprintf("%d%02d%02dT%02dx", t.Year(), t.Month(), t.Day(), t.Hour())
	}

	fileName := l.path + prefix
	if l.lgType == logAppend {
		return fileName + ".log"
	}

	if l.fileName == "" || l.size == 0 || l.size >= l.rollingSize {
		var (
			ok      bool
			err     error
			logName string
		)

		for i := 0; i < math.MaxInt32; i++ {
			logName, _ = filepath.Abs(fileName + fmt.Sprintf("%05d", i) + ".log")

			ok, err = fileExists(logName)
			if err == nil && !ok {
				l.fileName = logName
				return logName
			}
		}
	}
	return l.fileName
}

func (l *logger) write(data string) {
	if l != nil && l.enabled {
		ln := len(data)
		if ln > 0 {
			defer func() {
				if err := recover(); err != nil {
					fmt.Println(data)
				}
			}()

			fn := l.getFilename()

			f, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
			if err == nil && f != nil {
				defer f.Close()

				t := time.Now()
				nano := fmt.Sprintf("%05d", t.Nanosecond())
				if len(nano) > 5 {
					nano = nano[0:5]
				}

				date := fmt.Sprintf("[%02d-%02d-%04dT%02d:%02d:%02d.%s] ", t.Day(), t.Month(),
					t.Year(), t.Hour(), t.Minute(), t.Second(), nano)

				f.Write([]byte(date))
				f.Write([]byte(data))
				f.Write(newline)

				l.size += ln + 4
			}
		}
	}
}

func (l *logger) Console(enabled bool) {
	l.console = enabled
}

func (l *logger) Fatal(v ...interface{}) {
	if l != nil && l.enabled && len(v) > 0 {
		line := fmt.Sprint(v...)
		l.write(line)
		if l.console {
			fmt.Print(line)
		}
	}
	os.Exit(1)
}

func (l *logger) Fatalf(format string, v ...interface{}) {
	if l != nil && l.enabled && len(v) > 0 {
		line := fmt.Sprintf(format, v...)
		l.write(line)
		if l.console {
			fmt.Print(line)
		}
	}
	os.Exit(1)
}

func (l *logger) Fatalln(v ...interface{}) {
	if l != nil && l.enabled && len(v) > 0 {
		line := fmt.Sprintln(v...)
		l.write(line)
		if l.console {
			fmt.Print(line)
		}
	}
	os.Exit(1)
}

func (l *logger) Panic(v ...interface{}) {
	line := fmt.Sprint(v...)
	if l != nil && l.enabled {
		l.write(line)
		if l.console {
			fmt.Print(line)
		}
	}
	panic(line)
}

func (l *logger) Panicf(format string, v ...interface{}) {
	line := fmt.Sprintf(format, v...)
	if l != nil && l.enabled {
		l.write(line)
		if l.console {
			fmt.Print(line)
		}
	}
	panic(line)
}

func (l *logger) Panicln(v ...interface{}) {
	line := fmt.Sprintln(v...)
	if l != nil && l.enabled {
		l.write(line)
		if l.console {
			fmt.Print(line)
		}
	}
	panic(line)
}

func (l *logger) Print(v ...interface{}) {
	if l != nil && l.enabled && len(v) > 0 {
		line := fmt.Sprint(v...)
		l.write(line)
		if l.console {
			fmt.Print(line)
		}
	}
}

func (l *logger) Printf(format string, v ...interface{}) {
	if l != nil && l.enabled && len(v) > 0 {
		line := fmt.Sprintf(format, v...)
		l.write(line)
		if l.console {
			fmt.Print(line)
		}
	}
}

func (l *logger) Println(v ...interface{}) {
	if l != nil && l.enabled && len(v) > 0 {
		line := fmt.Sprintln(v...)
		l.write(line)
		if l.console {
			fmt.Print(line)
		}
	}
}
