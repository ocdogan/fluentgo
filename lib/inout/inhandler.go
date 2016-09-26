package inout

import (
	"github.com/ocdogan/fluentgo/lib"
)

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

func (ih *inHandler) getMaxMessageSize() int {
	msgSize := -1
	if ih.manager != nil {
		msgSize = ih.manager.GetMaxMessageSize()
	}
	return lib.MinInt(lib.InvalidMessageSize, lib.MaxInt(-1, msgSize))
}

func (ih *inHandler) queueMessage(data []byte, maxMsgSize int, compressed bool) {
	ln := len(data)
	if ln > 0 && (maxMsgSize < 1 || ln <= maxMsgSize) {
		defer recover()

		m := ih.GetManager()
		if m != nil {
			q := m.GetInQueue()

			if q != nil {
				if compressed {
					uncdata := lib.Decompress(data)
					if uncdata != nil {
						m := ih.GetManager()
						if m != nil {
							q.Push(uncdata)
							return
						}
					}
				}
				q.Push(data)
			}
		}
	}
}
