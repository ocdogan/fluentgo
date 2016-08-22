package main

import (
	"bytes"
	"compress/gzip"
	"os"
	"time"
)

const (
	InvalidMessageSize = 1024 * 1024
	ISO8601Time        = "2006-01-02T15:04:05.999-07:00"
)

func pathExists(path string) (bool, error) {
	fi, err := os.Stat(path)
	if err == nil {
		return fi.IsDir(), nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func fileExists(fileName string) (bool, error) {
	fi, err := os.Stat(fileName)
	if err == nil {
		return !fi.IsDir(), nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt16(a, b int16) int16 {
	if a < b {
		return a
	}
	return b
}

func maxInt16(a, b int16) int16 {
	if a > b {
		return a
	}
	return b
}

func minInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func maxInt32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func compress(data []byte) []byte {
	if len(data) > 0 {
		var buff bytes.Buffer
		gzipW := gzip.NewWriter(&buff)

		if gzipW != nil {
			defer gzipW.Close()

			n, err := gzipW.Write(data)
			if err != nil {
				return data
			} else if n > 0 {
				return buff.Bytes()
			}
		}
	}
	return nil
}
