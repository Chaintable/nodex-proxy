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
	"errors"
	"strings"
	"unsafe"

	"github.com/Chaintable/nodex-proxy/internal/jsonscan"
	"github.com/bytedance/sonic"
	"github.com/cloudwego/hertz/pkg/protocol"
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
//
// The returned objects borrow the request body: Jsonrpc, Method, Params and ID
// may point into it. Callers must not modify or release the body while the
// objects are in use, and must copy (see OwnRequests) anything that outlives
// the request or must survive a body rewrite.
func ParseRequest(r *protocol.Request) ([]byte, []*RequestObject, int, error) {
	body, err := r.BodyE()
	if err != nil {
		return nil, nil, 0, ErrReadBody
	}

	size := len(body)
	if reqs, ok := parseRequestBorrowed(body); ok {
		return body, reqs, size, nil
	}
	return parseRequestDecoded(body)
}

// parseRequestDecoded is the reference decoder. It copies the body, so its
// results never alias request memory; the borrowed fast path must agree with
// it on every input it accepts.
func parseRequestDecoded(body []byte) ([]byte, []*RequestObject, int, error) {
	size := len(body)
	reqs := make([]*RequestObject, 0)
	if IsBatch(body) {
		if err := sonic.Unmarshal(body, &reqs); err != nil {
			return nil, nil, size, ErrParseBatch
		}
		return body, reqs, size, nil
	}

	var req RequestObject
	if err := sonic.Unmarshal(body, &req); err != nil {
		return nil, nil, size, ErrParseSingle
	}
	reqs = append(reqs, &req)

	return body, reqs, size, nil
}

// singleRequest lets the common one-object request share one allocation with
// the slice that holds it.
type singleRequest struct {
	req  RequestObject
	reqs [1]*RequestObject
}

// parseRequestBorrowed decodes the common request shape without copying the
// body: string fields and raw params/id are views into it. It declines
// (ok=false) anything whose decoding is not byte-for-byte the raw text —
// escapes anywhere, non-ASCII or non-string jsonrpc/method, case-folded or
// non-ASCII keys, non-object batch items — and leaves those to the decoder,
// so both paths agree. sonic.Valid does not check escape sequences, which is
// why any backslash also falls back.
func parseRequestBorrowed(body []byte) ([]*RequestObject, bool) {
	if bytes.IndexByte(body, '\\') >= 0 || !sonic.Valid(body) {
		return nil, false
	}
	s := unsafe.String(unsafe.SliceData(body), len(body))
	i := jsonscan.SkipSpace(s, 0)
	if i >= len(s) {
		return nil, false
	}
	switch s[i] {
	case '{':
		single := &singleRequest{}
		if !scanRequestObject(s, body, i, &single.req) {
			return nil, false
		}
		single.reqs[0] = &single.req
		return single.reqs[:], true
	case '[':
		reqs := make([]*RequestObject, 0, 4)
		declined := false
		_, ok := jsonscan.ScanArray(s, i, func(_ int, vs, _ int) bool {
			req := &RequestObject{}
			if s[vs] != '{' || !scanRequestObject(s, body, vs, req) {
				declined = true
				return false
			}
			reqs = append(reqs, req)
			return true
		})
		if !ok || declined {
			return nil, false
		}
		return reqs, true
	default:
		return nil, false
	}
}

var requestKeys = [...]string{"jsonrpc", "method", "params", "id"}

func scanRequestObject(s string, body []byte, i int, req *RequestObject) bool {
	ok := true
	_, scanned := jsonscan.ScanObject(s, i, func(key string, keyEscaped bool, vs, ve int) bool {
		switch key {
		case "jsonrpc", "method":
			if s[vs] != '"' {
				ok = false
				return false
			}
			value := s[vs+1 : ve-1]
			if !jsonscan.IsPlainASCII(value) {
				ok = false
				return false
			}
			if key == "jsonrpc" {
				req.Jsonrpc = value
			} else {
				req.Method = RPCMethod(value)
			}
		case "params":
			req.Params = body[vs:ve:ve]
		case "id":
			req.ID = body[vs:ve:ve]
		default:
			// The decoder matches keys case-insensitively; leave any key that
			// could fold onto a field, or that we cannot fold, to it.
			if keyEscaped || !jsonscan.IsPlainASCII(key) {
				ok = false
				return false
			}
			for _, k := range requestKeys {
				if strings.EqualFold(key, k) {
					ok = false
					return false
				}
			}
		}
		return true
	})
	return ok && scanned
}

// OwnRequests replaces every borrowed field of reqs with a private copy, for
// callers about to rewrite the request body the objects were parsed from.
func OwnRequests(reqs []*RequestObject) {
	for _, req := range reqs {
		if req == nil {
			continue
		}
		req.Jsonrpc = strings.Clone(req.Jsonrpc)
		req.Method = RPCMethod(strings.Clone(string(req.Method)))
		if req.Params != nil {
			req.Params = bytes.Clone(req.Params)
		}
		if req.ID != nil {
			req.ID = bytes.Clone(req.ID)
		}
	}
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

var errorKeySuffix = []byte(`rror"`)

// ContainsErrorKey is a cheap pre-filter for response bodies: a JSON-RPC
// response without an "error"/"Error" key token cannot carry an error object,
// so callers can skip unmarshalling entirely. Keys with unicode-escaped names
// (e.g. "err\u006fr") are deliberately not recognized — no real node
// software emits them, and a false negative only skips retry/error accounting.
func ContainsErrorKey(msg []byte) bool {
	// Both accepted tokens share this suffix. Search for it once, then check
	// the preceding quote and e/E, instead of scanning large success results
	// twice. Keep this a conservative pre-filter: the existing unmarshallers
	// decide whether a match is actually a top-level error member.
	for offset := 0; offset < len(msg); {
		i := bytes.Index(msg[offset:], errorKeySuffix)
		if i < 0 {
			return false
		}
		i += offset
		if i >= 2 && msg[i-2] == '"' && (msg[i-1] == 'e' || msg[i-1] == 'E') {
			return true
		}
		offset = i + len(errorKeySuffix)
	}
	return false
}
