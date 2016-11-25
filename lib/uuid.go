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
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	sep = byte('-')
)

type UUID [16]byte

func (uuid UUID) String() string {
	result := make([]byte, 36)

	result[8] = sep
	result[13] = sep
	result[18] = sep
	result[23] = sep

	hex.Encode(result[0:8], uuid[0:4])
	hex.Encode(result[9:13], uuid[4:6])
	hex.Encode(result[14:18], uuid[6:8])
	hex.Encode(result[19:23], uuid[8:10])
	hex.Encode(result[24:], uuid[10:])

	return strings.ToUpper(string(result))
}

func (uuid UUID) StringWithoutSep() string {
	result := make([]byte, 32)

	hex.Encode(result[0:8], uuid[0:4])
	hex.Encode(result[8:12], uuid[4:6])
	hex.Encode(result[12:16], uuid[6:8])
	hex.Encode(result[16:20], uuid[8:10])
	hex.Encode(result[20:], uuid[10:])

	return strings.ToUpper(string(result))
}

type seqID struct {
	sync.Mutex
	id uint64
}

var (
	now   uint64
	euid  []byte
	haddr []byte
	id    = &seqID{}
)

func NewUUID() (*UUID, error) {
	uuid := new(UUID)
	_, err := rand.Read(uuid[:])
	if err != nil {
		return nil, err
	}

	uuid.xor(euid, 0)
	uuid.xor(getDate(), 8)
	uuid.xor(haddr, 4)

	return uuid, nil
}

func (uuid *UUID) xor(bytes []byte, shift int) {
	for i, b := range bytes {
		pos := i + shift
		if pos >= 16 {
			break
		}
		uuid[pos] ^= b
	}
}

func getDate() []byte {
	id.Lock()
	id.id++
	tick := id.id
	id.Unlock()

	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, now+tick)

	return result
}

func init() {
	defer recover()

	now = uint64(time.Now().Unix())

	euid = make([]byte, 8)
	binary.BigEndian.PutUint64(euid, uint64(os.Getpid()))

	nowArr := make([]byte, 8)
	binary.BigEndian.PutUint64(nowArr, now)

	for i, b := range euid {
		euid[i] = b ^ nowArr[i]
	}

	initHAddr()
}

func initHAddr() {
	if haddr != nil {
		return
	}

	infs, err := net.Interfaces()
	if err != nil {
		return
	}

	var lenAdd int
	for _, inf := range infs {
		lenH := len(inf.HardwareAddr)
		if lenH > 6 {
			if haddr == nil {
				lenAdd = lenH
				haddr = make([]byte, lenH)
				copy(haddr, inf.HardwareAddr)
			} else {
				l := lenH
				if lenAdd < l {
					l = lenAdd
				}

				for i, b := range inf.HardwareAddr {
					if i >= l {
						break
					}
					haddr[i] ^= b
				}
			}
		}
	}
}

func ParseUUID(uuid string) (*UUID, error) {
	var offsets1 [][]int
	if len(uuid) == 32 {
		offsets1 = [][]int{{0, 8}, {8, 12}, {12, 16}, {16, 20}, {20, len(uuid)}}
	} else if len(uuid) == 36 &&
		(uuid[8] == sep && uuid[13] == sep && uuid[18] == sep && uuid[23] == sep) {
		offsets1 = [][]int{{0, 8}, {9, 13}, {14, 18}, {19, 23}, {24, len(uuid)}}
	}

	if offsets1 != nil {
		var (
			err      error
			pair2    []int
			offsets2 = [][]int{{0, 4}, {4, 6}, {6, 8}, {8, 10}, {10, 16}}
		)

		result := new(UUID)

		var (
			spart string
			upart []byte
		)

		for i, pair1 := range offsets1 {
			pair2 = offsets2[i]

			spart = uuid[pair1[0]:pair1[1]]
			upart = result[pair2[0]:pair2[1]]

			_, err = hex.Decode(upart, []byte(spart))
			if err != nil {
				return nil, err
			}
		}

		return result, nil
	}

	return nil, fmt.Errorf("Not a valid UUID.")
}
