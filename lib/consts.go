package lib

const (
	In    = "in"
	Out   = "out"
	InOut = "inout"

	Publish = "PUBLISH"
	LPush   = "LPUSH"
	RPush   = "RPUSH"

	Subscribe       = "SUBSCRIBE"
	PSubscribe      = "PSUBSCRIBE"
	RPop            = "RPOP"
	LPop            = "LPOP"
	BrPop           = "BRPOP"
	BlPop           = "BLPOP"
	PSubscribechars = "*?[]"

	MinBufferSize  = 8 * 1024
	MaxBufferSize  = 10 * 1024 * 1024
	MinBufferCount = 10
	MaxBufferCount = 1000

	DefaultOutBulkCount = 50

	MinLogSize = 2 * 1024
	MaxLogSize = 10 * 1024 * 1024

	InvalidMessageSize = 1024 * 1024
	ISO8601Time        = "2006-01-02T15:04:05.999-07:00"

	TCPUDPMsgStart = "///*["
	TCPUDPMsgEnd   = "]*\\\\\\"
)
