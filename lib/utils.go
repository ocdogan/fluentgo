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

package lib

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func PathExists(path string) (bool, error) {
	fi, err := os.Stat(path)
	if err == nil {
		return fi.IsDir(), nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func FileExists(fileName string) (bool, error) {
	fi, err := os.Stat(fileName)
	if err == nil {
		return !fi.IsDir(), nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func MinInt16(a, b int16) int16 {
	if a < b {
		return a
	}
	return b
}

func MaxInt16(a, b int16) int16 {
	if a > b {
		return a
	}
	return b
}

func MinInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func MaxInt32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func MinDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func MaxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func PreparePath(dir string) string {
	dir = strings.TrimSpace(dir)
	if dir != "" {
		repSep := '/'
		if os.PathSeparator == '/' {
			repSep = '\\'
		}

		dir = strings.Replace(dir, string(repSep), string(os.PathSeparator), -1)
		dir = filepath.Clean(dir)
	}

	if dir == "" || dir == "." {
		dir = "." + string(os.PathSeparator)
	}

	absDir, err := filepath.Abs(dir)
	if err == nil {
		dir = absDir
	}

	if dir[len(dir)-1] != os.PathSeparator {
		dir += string(os.PathSeparator)
	}
	return dir
}

func LoadClientCert(certFile, keyFile string) (config *tls.Config, err error) {
	if certFile != "" && keyFile != "" {
		var cert tls.Certificate
		cert, err = tls.LoadX509KeyPair(certFile, keyFile)

		if err != nil {
			err = fmt.Errorf("Error loading client certificate: %v", err)
			return
		}

		config = &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}
		return
	}
	return nil, nil
}

func LoadServerCert(certFile, keyFile string) (config *tls.Config, err error) {
	keyFile = PrepareFile(keyFile)
	certFile = PrepareFile(certFile)

	if certFile != "" && keyFile != "" {
		var cert tls.Certificate
		cert, err = tls.LoadX509KeyPair(certFile, keyFile)

		if err != nil {
			err = fmt.Errorf("Error loading client certificate: %v", err)
			return nil, err
		}

		cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			err = fmt.Errorf("Error parsing client certificate: %v", err)
			return nil, err
		}

		config = &tls.Config{Certificates: []tls.Certificate{cert}}
		return
	}
	return nil, nil
}

func PrepareFile(filename string) string {
	filename = strings.TrimSpace(filename)
	if filename != "" {
		repSep := '/'
		if os.PathSeparator == '/' {
			repSep = '\\'
		}

		filename = strings.Replace(filename, string(repSep), string(os.PathSeparator), -1)
		filename = filepath.Clean(filename)

		if filename != "" {
			absFilename, err := filepath.Abs(filename)
			if err == nil {
				filename = absFilename
			}
		}
	}
	return filename
}

func Compress(data []byte) []byte {
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

func Decompress(data []byte) []byte {
	if len(data) > 0 {
		cmpReader, err := gzip.NewReader(bytes.NewReader(data))
		if err == nil && cmpReader != nil {
			defer cmpReader.Close()

			data, err = ioutil.ReadAll(cmpReader)
			if err != nil {
				return data
			}
		}
	}
	return nil
}
