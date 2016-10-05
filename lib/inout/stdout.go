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

import "fmt"

type stdOut struct {
	outHandler
}

func init() {
	RegisterOut("std", newStdOut)
	RegisterOut("stdout", newStdOut)
}

func newStdOut(manager InOutManager, params map[string]interface{}) OutSender {
	oh := newOutHandler(manager, params)
	if oh == nil {
		return nil
	}

	stdo := &stdOut{
		outHandler: *oh,
	}

	stdo.iotype = "STDOUT"

	stdo.runFunc = stdo.funcRunAndWait
	stdo.afterCloseFunc = stdo.funcAfterClose
	stdo.getDestinationFunc = stdo.funcGetObjectName
	stdo.sendChunkFunc = stdo.funcOutMessages

	return stdo
}

func (stdo *stdOut) funcAfterClose() {
	// Nothing to close
}

func (stdo *stdOut) funcGetObjectName() string {
	return "stdout"
}

func (stdo *stdOut) funcOutMessages(messages []string, indexName string) {
	if len(messages) > 0 {
		defer recover()

		for _, msg := range messages {
			if msg != "" {
				fmt.Println(msg)
			}
		}
	}
}

func (stdo *stdOut) funcRunAndWait() {
	defer func() {
		recover()
		l := stdo.GetLogger()
		if l != nil {
			l.Println("Stoping 'STDOUT'...")
		}
	}()

	l := stdo.GetLogger()
	if l != nil {
		l.Println("Starting 'STDOUT'...")
	}

	<-stdo.completed
}
