package main

const (
	in    = "in"
	out   = "out"
	inout = "inout"

	publish = "PUBLISH"
	lpush   = "LPUSH"
	rpush   = "RPUSH"

	subs       = "SUBSCRIBE"
	psubs      = "PSUBSCRIBE"
	psubschars = "*?"

	minBufferSize  = 8 * 1024
	maxBufferSize  = 10 * 1024 * 1024
	minBufferCount = 10
	maxBufferCount = 1000
)
