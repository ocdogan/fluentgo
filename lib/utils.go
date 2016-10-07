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
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
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

func MinFloat32(a, b float32) float32 {
	if a < b {
		return a
	}
	return b
}

func MaxFloat32(a, b float32) float32 {
	if a > b {
		return a
	}
	return b
}

func MinFloat64(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func MaxFloat64(a, b float64) float64 {
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

func LoadClientCert(certFile, keyFile, caFile string, verifySsl bool) (config *tls.Config, err error) {
	if certFile != "" && keyFile != "" {
		var cert tls.Certificate
		cert, err = tls.LoadX509KeyPair(certFile, keyFile)

		if err != nil {
			err = fmt.Errorf("Error loading client certificate: %v", err)
			return
		}

		var caCertPool *x509.CertPool
		if caFile != "" {
			var caCert []byte
			caCert, err = ioutil.ReadFile(caFile)
			if err != nil {
				err = fmt.Errorf("Error parsing server certificate: %v", err)
				return nil, err
			}

			caCertPool = x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
		}

		config = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: verifySsl,
		}
		return
	}
	return nil, nil
}

func LoadServerCert(certFile, keyFile, caFile string, verifySsl bool) (config *tls.Config, err error) {
	keyFile = PrepareFile(keyFile)
	certFile = PrepareFile(certFile)

	if certFile == "" || keyFile == "" {
		return nil, nil
	}

	var cert tls.Certificate
	cert, err = tls.LoadX509KeyPair(certFile, keyFile)

	if err != nil {
		err = fmt.Errorf("Error loading server certificate: %v", err)
		return nil, err
	}

	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		err = fmt.Errorf("Error parsing server certificate: %v", err)
		return nil, err
	}

	var caCertPool *x509.CertPool
	if caFile != "" {
		var caCert []byte
		caCert, err = ioutil.ReadFile(caFile)
		if err != nil {
			err = fmt.Errorf("Error parsing server certificate: %v", err)
			return nil, err
		}

		caCertPool = x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
	}

	config = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: verifySsl,
	}

	return
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

func Compress(data []byte, compType CompressionType) []byte {
	if len(data) > 0 {
		var (
			buff    bytes.Buffer
			zWriter io.Writer
			closer  io.Closer
		)

		if compType == CtZip {
			zw := zip.NewWriter(&buff)
			closer = zw

			header := &zip.FileHeader{
				Name:         "comressed.dat",
				Method:       zip.Store,
				ModifiedTime: uint16(time.Now().UnixNano()),
				ModifiedDate: uint16(time.Now().UnixNano()),
			}

			iow, err := zw.CreateHeader(header)
			if err != nil {
				return data
			}

			zWriter = iow
		} else {
			zw := gzip.NewWriter(&buff)
			closer = zw

			zWriter = zw
		}

		if zWriter != nil {
			defer func() {
				if closer != nil {
					closer.Close()
				}
			}()

			n, err := zWriter.Write(data)
			if err != nil {
				return data
			} else if n > 0 {
				return buff.Bytes()
			}
		}
	}
	return nil
}

func Decompress(data []byte, compType CompressionType) []byte {
	if len(data) > 0 {
		var (
			closer  io.Closer
			zReader io.Reader
		)

		if compType == CtZip {
			zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				return data
			}

			if len(zr.File) == 0 {
				return data
			}

			iorc, err := zr.File[0].Open()
			if err != nil {
				return data
			}

			zReader = iorc
			closer = iorc
		} else {
			zr, err := gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				return data
			}

			zReader = zr
			closer = zr
		}

		if zReader != nil {
			defer func() {
				if closer != nil {
					closer.Close()
				}
			}()

			cdata, err := ioutil.ReadAll(zReader)
			if err != nil {
				return data
			}
			return cdata
		}
	}
	return data
}
