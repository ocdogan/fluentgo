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

package log

import (
	"fmt"
	"os"
)

type dummyLogger struct {
}

func NewDummyLogger() *dummyLogger {
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
