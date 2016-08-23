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

func (rio *redisIO) tryToCloseConn(conn redis.Conn) error {
	var closeErr error
	if conn != nil {
		defer func() {
			err := recover()
			if closeErr == nil {
				closeErr, _ = err.(error)
			}
		}()
		closeErr = conn.Close()
	}
	return closeErr
}

func (rio *redisIO) selectDb(conn redis.Conn) error {
	var connErr error
	defer func() {
		err := recover()
		if err != nil && connErr == nil {
			defer recover()
			connErr, _ = err.(error)
			rio.tryToCloseConn(conn)
		}
	}()

	_, connErr = conn.Do("SELECT", rio.db)
	return connErr
}

func (rio *redisIO) runConnFunc(conn redis.Conn) error {
	var funcErr error

	cfn := rio.connFunc
	if cfn != nil {
		defer func() {
			err := recover()
			if err != nil && funcErr == nil {
				defer recover()
				funcErr, _ = err.(error)
				conn.Close()
			}
		}()

		funcErr = cfn(conn)
	}
	return funcErr
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
			connErr = rio.selectDb(conn)
			if connErr != nil && rio.logger != nil {
				rio.logger.Println(connErr)
			}

			connErr = rio.runConnFunc(conn)
		}

		if connErr == nil {
			rio.conn = conn
		} else {
			rio.conn = nil
			rio.tryToCloseConn(conn)
		}
	}
}
