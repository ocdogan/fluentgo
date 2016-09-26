package lib

type LogType int

const (
	LogRolling LogType = iota
	LogAppend
)
