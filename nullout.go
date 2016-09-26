package main

type nullOut struct {
	outHandler
}

func newNullOut(manager InOutManager, config *inOutConfig) *nullOut {
	if config == nil {
		return nil
	}

	params := config.getParamsMap()

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
