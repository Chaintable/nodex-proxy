// Copyright (c) 2022 DeBank Inc. <admin@debank.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package jsonrpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

var (
	// ErrReadBody ...
	ErrReadBody = errors.New("failed to read jsonrpc request body")
	// ErrEmptyBody ...
	ErrEmptyBody = errors.New("empty jsonrpc request body")
	// ErrParseBatch ...
	ErrParseBatch = errors.New("failed to parse jsonrpc batch request")
	// ErrParseSingle ...
	ErrParseSingle = errors.New("failed to parse jsonrpc request")
	// ErrInvalidRequest ...
	ErrInvalidRequest = errors.New("invalid jsonrpc request")
)

// ParseRequest parse the jsonrpc request, unpack it into one RequestObject or
// more to []RequestObject.
func ParseRequest(r *http.Request) ([]byte, []*RequestObject, int, error) {
	if r.Body == nil {
		return nil, nil, 0, ErrEmptyBody
	}
	body, err := io.ReadAll(r.Body)

	// handle whether err is encountered or not
	r.Body.Close()
	r.Body = io.NopCloser(bytes.NewBuffer(body))
	if err != nil {
		return nil, nil, 0, ErrReadBody
	}

	size := len(body)
	reqs := make([]*RequestObject, 0)
	switch {
	case IsBatch(body):
		if err := json.Unmarshal(body, &reqs); err != nil {
			return nil, nil, size, ErrParseBatch
		}
		return body, reqs, size, nil
	default:
	}

	var req RequestObject
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, nil, size, ErrParseSingle
	}
	reqs = append(reqs, &req)

	return body, reqs, size, nil
}

// ValidateRequest check the value of jsonrpc request is valid or not.
func ValidateRequest(req *RequestObject) error {
	if req == nil {
		return &ErrorObject{
			Code:    ParseErrorCode,
			Message: ParseErrorMsg,
			Data:    "request must not be nil",
		}
	}
	if req.Jsonrpc != "2.0" {
		return &ErrorObject{
			Code:    InvalidRequestCode,
			Message: InvalidRequestMsg,
			Data:    "protocol must be exactly 2.0",
		}
	}
	if req.Method == "" {
		return &ErrorObject{
			Code:    InvalidRequestCode,
			Message: InvalidRequestMsg,
			Data:    "method must not empty",
		}
	}

	return nil
}

func IsBatch(msg []byte) bool {
	for _, c := range msg {
		if c == 0x20 || c == 0x09 || c == 0x0a || c == 0x0d {
			continue
		}
		return c == '['
	}
	return false
}
