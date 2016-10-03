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

type nullOut struct {
	outHandler
}

func newNullOut(manager InOutManager, params map[string]interface{}) *nullOut {
	oh := newOutHandler(manager, params)
	if oh == nil {
		return nil
	}

	nullo := &nullOut{
		outHandler: *oh,
	}

	nullo.iotype = "NULLOUT"

	nullo.runFunc = nullo.funcRunAndWait
	nullo.afterCloseFunc = nullo.funcAfterClose
	nullo.getDestinationFunc = nullo.funcGetObjectName
	nullo.sendChunkFunc = nullo.funcPutMessages

	return nullo
}

func (nullo *nullOut) funcAfterClose() {
}

func (nullo *nullOut) funcGetObjectName() string {
	return "null"
}

func (nullo *nullOut) funcPutMessages(messages []string, indexName string) {
}

func (nullo *nullOut) funcRunAndWait() {
	defer func() {
		recover()
		l := nullo.GetLogger()
		if l != nil {
			l.Println("Stoping 'NULLOUT'...")
		}
	}()

	l := nullo.GetLogger()
	if l != nil {
		l.Println("Starting 'NULLOUT'...")
	}

	<-nullo.completed
}
