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

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/ocdogan/fluentgo/config"
	"github.com/ocdogan/fluentgo/lib"
	"github.com/ocdogan/fluentgo/log"
	"gopkg.in/mgo.v2"
)

type mongOut struct {
	sync.Mutex
	outHandler
	servers        string
	db             string
	dialTimeout    time.Duration
	collectionPath *lib.JsonPath
	lg             log.Logger
	session        *mgo.Session
}

func init() {
	RegisterOut("mongo", newMongOut)
	RegisterOut("mongout", newMongOut)
	RegisterOut("mongoout", newMongOut)
}

func newMongOut(manager InOutManager, params map[string]interface{}) OutSender {
	oh := newOutHandler(manager, params)
	if oh == nil {
		return nil
	}

	servers, ok := config.ParamAsString(params, "servers")
	if !ok || servers == "" {
		return nil
	}

	db, ok := config.ParamAsString(params, "db")
	if !ok || db == "" {
		return nil
	}

	collection, ok := config.ParamAsString(params, "collection")
	if !ok || collection == "" {
		return nil
	}

	dialTimeout, ok := config.ParamAsDurationWithLimit(params, "dialTimeoutMSec", 0, 60000)
	if ok {
		dialTimeout *= time.Millisecond
	}

	collectionPath := lib.NewJsonPath(collection)
	if collectionPath == nil {
		return nil
	}

	mo := &mongOut{
		outHandler:     *oh,
		servers:        servers,
		db:             db,
		dialTimeout:    dialTimeout,
		collectionPath: collectionPath,
		lg:             manager.GetLogger(),
	}

	mo.iotype = "MONGOUT"

	mo.runFunc = mo.waitComplete
	mo.afterCloseFunc = mo.funcAfterClose
	mo.getDestinationFunc = mo.funcGetObjectName
	mo.sendChunkFunc = mo.funcPutMessages

	return mo
}

func (mo *mongOut) funcAfterClose() {
	if mo.session != nil {
		defer recover()

		mo.Lock()
		defer mo.Unlock()

		if mo.session != nil {
			session := mo.session
			mo.session = nil

			session.Close()
		}
	}
}

func (mo *mongOut) funcGetObjectName() string {
	return "null"
}

func (mo *mongOut) funcPutMessages(messages []ByteArray, _ string) {
	if len(messages) == 0 {
		return
	}
	defer recover()

	var (
		collection     string
		collectionList []interface{}
		collections    = make(map[string][]interface{})
		useDefaultCol  = mo.collectionPath.IsStatic()
	)

	if useDefaultCol {
		epath, err := mo.collectionPath.Eval(nil, true)
		if err != nil || epath == nil {
			return
		}

		col, ok := epath.(string)
		if !ok || len(col) == 0 {
			return
		}

		collection = col
	}

	for _, msg := range messages {
		if len(msg) > 0 {
			var jsonMsg map[string]interface{}

			err := json.Unmarshal([]byte(msg), &jsonMsg)
			if err != nil || jsonMsg == nil {
				continue
			}

			if !useDefaultCol {
				collection = ""

				epath, err := mo.collectionPath.Eval(jsonMsg, true)
				if err != nil || epath == nil {
					return
				}

				col, ok := epath.(string)
				if !ok || len(col) == 0 {
					continue
				}
				collection = col
			}

			if len(collection) > 0 {
				collectionList, _ = collections[collection]
				collections[collection] = append(collectionList, jsonMsg)
			}
		}
	}

	if len(collections) > 0 {
		err := mo.Connect()
		if err != nil {
			l := mo.GetLogger()
			if l != nil {
				l.Printf("Unable to connect to server '%s' for MONGOUT message to %s:%s: %s", mo.servers, mo.db, collection, err)
			}
			return
		}

		var (
			mgoCol *mgo.Collection
			mgoDb  = mo.session.DB(mo.db)
		)

		for collection, collectionList = range collections {
			if len(collectionList) > 0 && len(collection) > 0 {
				func() {
					defer recover()

					if mgoCol == nil || mgoCol.Name != collection {
						mgoCol = mgoDb.C(collection)
					}

					var mgoErr error
					if len(collectionList) == 1 {
						mgoErr = mgoCol.Insert(collectionList...)
					} else {
						bulk := mgoCol.Bulk()
						bulk.Unordered()

						bulk.Insert(collectionList...)
						_, mgoErr = bulk.Run()
					}

					if mgoErr != nil {
						l := mo.GetLogger()
						if l != nil {
							l.Printf("Cannot send MONGOUT message to %s:%s: %s", mo.db, collection, mgoErr)
						}
					}
				}()
			}
		}
	}
}

func (mo *mongOut) Connect() error {
	if mo.session == nil {
		mo.Lock()
		defer mo.Unlock()

		if mo.session == nil {
			var (
				session *mgo.Session
				err     error
			)

			if mo.dialTimeout == 0 {
				session, err = mgo.Dial(mo.servers)
			} else {
				session, err = mgo.DialWithTimeout(mo.servers, mo.dialTimeout)
			}

			if err != nil {
				if mo.lg != nil {
					mo.lg.Printf("Failed to create MONGOUT session: %s\n", err)
				}
				return err
			}

			// Optional. Switch the session to a monotonic behavior.
			session.SetMode(mgo.Monotonic, true)

			mo.session = session
		}
	}
	return nil
}
