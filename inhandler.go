package main

type inHandler struct {
	ioHandler
}

func newInHandler(manager InOutManager, params map[string]interface{}) *inHandler {
	ioh := newIOHandler(manager, params)
	if ioh == nil {
		return nil
	}

	return &inHandler{
		ioHandler: *ioh,
	}
}

func (ih *inHandler) queueMessage(data []byte, maxMsgSize int, compressed bool) {
	ln := len(data)
	if ln > 0 && (maxMsgSize < 1 || ln <= maxMsgSize) {
		defer recover()

		if compressed {
			uncdata := decompress(data)
			if uncdata != nil {
				ih.GetManager().GetQueue().Push(uncdata)
				return
			}
		}
		ih.GetManager().GetQueue().Push(data)
	}
}
