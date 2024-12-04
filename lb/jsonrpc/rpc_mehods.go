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
	"context"
	"fmt"

	spec "github.com/Chaintable/nodex-proxy/jsonrpc"
)

// RPCMethodCacheHandler ...
type RPCMethodCacheHandler interface {
	GetRPCMethod(context.Context, *spec.RequestObject) (*spec.ResponseObject, error)
	PutRPCMethod(context.Context, *spec.RequestObject, *spec.ResponseObject) error
}

// ImmutableMethodCacheHandler ...
type ImmutableMethodCacheHandler struct {
	cache interface{} // use interface{} rather than cache.Cache is more efficient
}

func (h *ImmutableMethodCacheHandler) cacheable() bool {
	return true
}

func (h *ImmutableMethodCacheHandler) cacheKey(req *spec.RequestObject) string {
	return fmt.Sprintf("method:%s", req.Method)
}

func (h *ImmutableMethodCacheHandler) cacheExist() bool {
	return h.cache != nil
}

func (h *ImmutableMethodCacheHandler) GetRPCMethod(ctx context.Context, req *spec.RequestObject) (*spec.ResponseObject, error) {
	if !h.cacheable() {
		return nil, fmt.Errorf("rpc method %s not cacheable", req.Method)
	}

	if !h.cacheExist() {
		return nil, nil
	}

	return &spec.ResponseObject{
		Jsonrpc: req.Jsonrpc,
		Result:  h.cache,
		ID:      req.ID,
	}, nil
}

func (h *ImmutableMethodCacheHandler) PutRPCMethod(ctx context.Context, req *spec.RequestObject, resp *spec.ResponseObject) error {
	if !h.cacheable() {
		return fmt.Errorf("rpc method %s not cacheable", req.Method)
	}

	if h.cacheExist() {
		return nil
	}

	h.cache = resp.Result
	return nil
}
