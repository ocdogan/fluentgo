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
	"net"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	pools = make(map[string]*redis.Pool)
)

func getRedisConnection(poolName, server, password string, options ...redis.DialOption) redis.Conn {
	key := poolName + ":" + server + ":" + password

	pool, ok := pools[key]
	if !ok {
		pool = &redis.Pool{
			MaxIdle:     3,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				connOpt := redis.DialNetDial(func(network, address string) (net.Conn, error) {
					var d net.Dialer
					// You can set any TCP socket level option here,
					// such as KeepAlive options, eighter via Dialer or via returned net.Conn
					return d.Dial(network, address)
				})

				options = append(options, connOpt)

				c, err := redis.Dial("tcp", server, options...)
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
		pools[key] = pool
	}
	return pool.Get()
}
