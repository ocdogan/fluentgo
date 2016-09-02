package main

const (
	in    = "in"
	out   = "out"
	inout = "inout"

	publish = "PUBLISH"
	lpush   = "LPUSH"
	rpush   = "RPUSH"

	subscribe       = "SUBSCRIBE"
	psubscribe      = "PSUBSCRIBE"
	psubscribechars = "*?[]"

	minBufferSize  = 8 * 1024
	maxBufferSize  = 10 * 1024 * 1024
	minBufferCount = 10
	maxBufferCount = 1000

	defaultOutBulkCount = 50

	minLogSize = 2 * 1024
	maxLogSize = 10 * 1024 * 1024

	InvalidMessageSize = 1024 * 1024
	ISO8601Time        = "2006-01-02T15:04:05.999-07:00"
)
