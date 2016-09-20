package main

import (
	"fmt"
	"os"
)

type dummyLogger struct {
}

func newDummyLogger() *dummyLogger {
	return &dummyLogger{}
}

func (l *dummyLogger) Fatal(v ...interface{}) {
	if l != nil && len(v) > 0 {
		line := fmt.Sprint(v...)
		fmt.Print(line)
	}
	os.Exit(1)
}

func (l *dummyLogger) Fatalf(format string, v ...interface{}) {
	if l != nil && len(v) > 0 {
		line := fmt.Sprintf(format, v...)
		fmt.Print(line)
	}
	os.Exit(1)
}

func (l *dummyLogger) Fatalln(v ...interface{}) {
	if l != nil && len(v) > 0 {
		line := fmt.Sprintln(v...)
		fmt.Print(line)
	}
	os.Exit(1)
}

func (l *dummyLogger) Panic(v ...interface{}) {
	line := fmt.Sprint(v...)
	if l != nil {
		fmt.Print(line)
	}
	panic(line)
}

func (l *dummyLogger) Panicf(format string, v ...interface{}) {
	line := fmt.Sprintf(format, v...)
	if l != nil {
		fmt.Print(line)
	}
	panic(line)
}

func (l *dummyLogger) Panicln(v ...interface{}) {
	line := fmt.Sprintln(v...)
	if l != nil {
		fmt.Print(line)
	}
	panic(line)
}

func (l *dummyLogger) Print(v ...interface{}) {
	if l != nil && len(v) > 0 {
		line := fmt.Sprint(v...)
		fmt.Print(line)
	}
}

func (l *dummyLogger) Printf(format string, v ...interface{}) {
	if l != nil && len(v) > 0 {
		line := fmt.Sprintf(format, v...)
		fmt.Print(line)
	}
}

func (l *dummyLogger) Println(v ...interface{}) {
	if l != nil && len(v) > 0 {
		line := fmt.Sprintln(v...)
		fmt.Print(line)
	}
}

func (l *dummyLogger) Log(v ...interface{}) {
	l.Print(v)
}

func (l *dummyLogger) Logf(format string, v ...interface{}) {
	l.Printf(format, v)
}

func (l *dummyLogger) Logln(v ...interface{}) {
	l.Println(v)
}
