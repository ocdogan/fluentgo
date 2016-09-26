package inout

import (
	"fmt"

	"github.com/ocdogan/fluentgo/lib/config"
)

type stdOut struct {
	outHandler
}

func newStdOut(manager InOutManager, config *config.InOutConfig) *stdOut {
	if config == nil {
		return nil
	}

	params := config.GetParamsMap()

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
