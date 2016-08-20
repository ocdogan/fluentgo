package main

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	pools = make(map[string]*redis.Pool)
)

func getPool(server, password string, logger Logger) *redis.Pool {
	pool, ok := pools[server+":"+password]
	if !ok {
		pool = &redis.Pool{
			MaxIdle:     3,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", server)
				if err != nil {
					return nil, err
				}
				if password != "" {
					_, err := c.Do("AUTH", password)
					if err != nil {
						c.Close()
						return nil, err
					}
				}
				return c, err
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		}
		pools[server+":"+password] = pool
	}
	return pool
}
