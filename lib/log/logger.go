package log

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/lib/config"
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

	Log(v ...interface{})
	Logf(format string, v ...interface{})
	Logln(v ...interface{})
}

type logger struct {
	sync.Mutex
	enabled     bool
	path        string
	fileName    string
	rollingSize int
	size        int
	console     bool
	lgType      lib.LogType
}

func init() {
	if runtime.GOOS == "windows" {
		newline = []byte("\r\n")
	}
}

func NewLogger(config *config.LogConfig) *logger {
	lt := lib.LogAppend
	if config.Type == "rolling" {
		lt = lib.LogRolling
	}

	logPath := config.GetPath()

	if ok, err := lib.PathExists(logPath); !ok || err != nil {
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

func getRollingSize(config *config.LogConfig) int {
	sz := config.RollingSize
	if sz < lib.MinLogSize {
		sz = lib.MinLogSize
	} else if sz > lib.MaxLogSize {
		sz = lib.MaxLogSize
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
	if l.lgType == lib.LogAppend {
		prefix = fmt.Sprintf("%d%02d%02d", t.Year(), t.Month(), t.Day())
	} else {
		prefix = fmt.Sprintf("%d%02d%02dT%02dx", t.Year(), t.Month(), t.Day(), t.Hour())
	}

	fileName := l.path + prefix
	if l.lgType == lib.LogAppend {
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

			ok, err = lib.FileExists(logName)
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

func (l *logger) Log(v ...interface{}) {
	l.Print(v)
}

func (l *logger) Logf(format string, v ...interface{}) {
	l.Printf(format, v)
}

func (l *logger) Logln(v ...interface{}) {
	l.Println(v)
}
