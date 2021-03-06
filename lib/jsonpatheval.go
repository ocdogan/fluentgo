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
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/oliveagle/jsonpath"
)

type JsonPathPartType int

const (
	JPPStatic JsonPathPartType = iota
	JPPDynamic
)

func (jt JsonPathPartType) String() string {
	if jt == JPPStatic {
		return "Static"
	}
	return "Dynamic"
}

type JsonPathPart struct {
	Data  string
	Type  JsonPathPartType
	Start int
	Len   int
}

func (jp *JsonPathPart) IsStatic() bool {
	return jp.Type == JPPStatic
}

func (jp *JsonPathPart) String() string {
	return fmt.Sprintf("{\"Data\": \"%s\", \"Type\": \"%s\", \"Start\": %d, \"Len\": %d}", jp.Data, jp.Type, jp.Start, jp.Len)
}

type JsonPathType int

const (
	JPStatic JsonPathType = iota
	JPComplex
)

func (jt JsonPathType) String() string {
	if jt == JPStatic {
		return "Static"
	}
	return "Complex"
}

type JsonPath struct {
	Type  JsonPathType
	Parts []JsonPathPart
}

func (jp *JsonPath) IsStatic() bool {
	return jp.Type == JPStatic
}

func (jp *JsonPath) String() string {
	buf := bytes.NewBuffer(nil)
	if len(jp.Parts) > 0 {
		for _, p := range jp.Parts {
			buf.WriteString(p.Data)
		}
	}
	return buf.String()
}

var rgx *regexp.Regexp

func init() {
	rgx = regexp.MustCompile(`\%\{[^}%]*\}\%`)
}

func NewJsonPath(s string) *JsonPath {
	jp := &JsonPath{Type: JPStatic}

	if len(s) == 0 {
		ep := JsonPathPart{
			Data: s,
			Type: JPPStatic,
		}
		jp.Parts = append(jp.Parts, ep)

		return jp
	}

	r := rgx.Copy()
	if r == nil {
		ep := JsonPathPart{
			Data: s,
			Type: JPPStatic,
		}
		jp.Parts = append(jp.Parts, ep)
	} else {
		mi := r.FindAllStringIndex(s, -1)
		if len(mi) == 0 {
			ep := JsonPathPart{
				Data: s,
				Type: JPPStatic,
			}
			jp.Parts = append(jp.Parts, ep)
		} else {
			jp.Type = JPComplex

			var (
				s1   string
				prev []int
			)

			for i, m := range mi {
				if i == 0 {
					if len(s1) > 0 {
						ep := JsonPathPart{
							Data:  s1,
							Type:  JPPStatic,
							Start: 0,
							Len:   m[0],
						}
						jp.Parts = append(jp.Parts, ep)
					}
				} else {
					prev = mi[i-1]
					if prev[1] < m[0] {
						s1 = s[prev[1]:m[0]]
						if len(s1) > 0 {
							ep := JsonPathPart{
								Data:  s1,
								Type:  JPPStatic,
								Start: prev[1],
								Len:   m[0] - prev[1],
							}
							jp.Parts = append(jp.Parts, ep)
						}
					}
				}

				s1 = s[m[0]:m[1]]
				if len(s1) > 0 {
					s1 = s[m[0]:m[1]]
					ep := JsonPathPart{
						Data:  s1,
						Type:  JPPDynamic,
						Start: m[0],
						Len:   m[1] - m[0],
					}
					jp.Parts = append(jp.Parts, ep)
				}
			}

			last := mi[len(mi)-1]
			lastLen := len(s) - last[1]

			if lastLen > 1 {
				s1 = s[last[1]:len(s)]

				ep := JsonPathPart{
					Data:  s1,
					Type:  JPPStatic,
					Start: last[1],
					Len:   len(s) - last[1],
				}
				jp.Parts = append(jp.Parts, ep)
			}
		}
	}

	return jp
}

func (jp *JsonPath) evalJson(jsonData interface{}, trimSpace bool) (result string, err error) {
	var (
		s     string
		jpath string
	)

	b := bytes.NewBuffer(nil)

	for _, p := range jp.Parts {
		s = p.Data
		if trimSpace {
			s = strings.TrimSpace(s)
		}

		if len(s) > 0 {
			if p.IsStatic() {
				b.WriteString(s)
			} else if len(s) > 4 {
				jpath = s[2 : len(s)-2]
				if trimSpace {
					jpath = strings.TrimSpace(jpath)
				}

				if len(jpath) > 0 {
					res, err := jsonpath.JsonPathLookup(jsonData, jpath)
					if err != nil {
						return "", err
					} else if res == nil {
						b.WriteString(jpath)
					} else {
						switch res.(type) {
						case string:
						case float64:
						case bool:
							jpath = fmt.Sprint(res)
							if trimSpace {
								jpath = strings.TrimSpace(jpath)
							}

							if len(jpath) > 0 {
								b.WriteString(jpath)
							}
						default:
							return "", fmt.Errorf("Cannot evaluate path: %s.", jpath)
						}
					}
				}
			}
		}
	}
	return b.String(), nil
}

func (jp *JsonPath) Eval(jsonData interface{}, trimSpace bool) (interface{}, error) {
	if jp == nil {
		return nil, fmt.Errorf("Json path can not be nil")
	}

	if jp.IsStatic() {
		if len(jp.Parts) > 0 {
			part := jp.Parts[0]
			if trimSpace {
				result := strings.TrimSpace(part.Data)
				return result, nil
			}
			return part.Data, nil
		}
		return "", nil
	}

	if jsonData == nil {
		return nil, fmt.Errorf("Unable to evaluate nil json data")
	}

	s, ok := jsonData.(string)
	if ok {
		if len(s) == 0 {
			return "", nil
		}

		var data interface{}
		err := json.Unmarshal([]byte(s), &data)
		if err != nil {
			return jsonData, nil
		}
	}

	return jp.evalJson(jsonData, trimSpace)
}

func (jp *JsonPath) EvalString(jsonData string, trimSpace bool) (result interface{}, err error) {
	if jp.IsStatic() {
		var s string
		if len(jp.Parts) > 0 {
			s = jp.Parts[0].Data
			if trimSpace {
				s = strings.TrimSpace(s)
			}
		}
		return s, nil
	}

	if len(jsonData) == 0 {
		return "", nil
	}

	var data interface{}
	err = json.Unmarshal([]byte(jsonData), &data)
	if err != nil {
		return data, nil
	}

	result, err = jp.evalJson(data, trimSpace)
	return
}
