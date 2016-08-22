package main

import (
	"reflect"
	"strings"

	"github.com/garyburd/redigo/redis"
)

type redisIO struct {
	db         int
	command    string
	server     string
	password   string
	channel    string
	compressed bool
	connFunc   func(redis.Conn) error
	conn       redis.Conn
	logger     Logger
}

func newRedisIO(logger Logger, params map[string]interface{}) *redisIO {
	if params == nil {
		return nil
	}

	var (
		ok       bool
		db       int
		s        string
		command  string
		server   string
		channel  string
		password string
	)

	s, ok = params["server"].(string)
	if ok {
		server = strings.TrimSpace(s)
	}
	if server == "" {
		return nil
	}

	s, ok = params["channel"].(string)
	if ok {
		channel = strings.TrimSpace(s)
	}
	if channel == "" {
		return nil
	}

	s, ok = params["password"].(string)
	if ok {
		password = strings.TrimSpace(s)
	}

	s, ok = params["command"].(string)
	if ok {
		command = strings.ToUpper(strings.TrimSpace(s))
	}

	db, ok = params["db"].(int)
	if ok {
		db = minInt(15, maxInt(0, db))
	}

	var compressed bool
	compressed, ok = params["compressed"].(bool)

	rio := &redisIO{
		db:         db,
		command:    command,
		server:     server,
		password:   password,
		compressed: compressed,
		channel:    channel,
		logger:     logger,
	}

	return rio
}

func (rio *redisIO) funcAfterClose() {
	defer recover()

	conn := rio.conn
	if conn != nil {
		rio.conn = nil
		conn.Close()
	}
}

func (rio *redisIO) tryToCloseConn(conn redis.Conn) {
	if conn != nil {
		defer recover()
		conn.Close()
	}
}

func (rio *redisIO) Connect() {
	defer func() {
		if err := recover(); err != nil {
			if rio.logger != nil {
				rio.logger.Panic(err)
			}
		}
	}()

	conn := rio.conn
	hasConn := conn != nil && !reflect.ValueOf(conn).IsNil()

	var connErr error

	if hasConn {
		connErr = conn.Err()
		if connErr != nil {
			rio.conn = nil
		}
	}

	if connErr != nil || !hasConn {
		if hasConn {
			rio.tryToCloseConn(conn)
		}

		connErr = nil
		conn = getPool(rio.server, rio.password, rio.logger).Get()

		if conn != nil {
			conn.Do("SELECT", rio.db)

			if rio.connFunc != nil {
				connErr = func() error {
					var connFuncErr error
					defer func() {
						err := recover()
						if err != nil {
							defer recover()
							connFuncErr, _ = err.(error)
							conn.Close()
						}
					}()

					connFuncErr = rio.connFunc(conn)
					return connFuncErr
				}()

				if connErr != nil && rio.logger != nil {
					rio.logger.Println(connErr)
				}
			}
		}

		if connErr == nil {
			rio.conn = conn
		} else {
			rio.conn = nil
			rio.tryToCloseConn(conn)
		}
	}
}
