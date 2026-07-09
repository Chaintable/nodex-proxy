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
	"net/http"
	"sync"

	json "github.com/bytedance/sonic"

	"github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/go-chi/chi/v5"
	"golang.org/x/time/rate"
)

type Limiter interface {
	Allow(method jsonrpc.RPCMethod) bool
	UpdateLimit(key jsonrpc.RPCMethod, limit int) bool
}

type methodLimiter struct {
	limiters map[jsonrpc.RPCMethod]*rate.Limiter
	sync.RWMutex
}

// NewMethodLimiter ...
func NewMethodLimiter(conf map[string]int) Limiter {
	limiters := make(map[jsonrpc.RPCMethod]*rate.Limiter)
	for method, rps := range conf {
		if method != "" && rps != 0 {
			limiters[jsonrpc.RPCMethod(method)] = rate.NewLimiter(rate.Limit(rps), rps)
		}
	}
	return &methodLimiter{
		limiters: limiters,
	}
}

func (ls *methodLimiter) UpdateLimit(method jsonrpc.RPCMethod, rps int) bool {
	if method == "" || rps < 0 {
		return false
	}

	switch rps {
	case 0: // disable rate limit
		ls.Lock()
		delete(ls.limiters, method)
		ls.Unlock()
	default:
		limit := rate.Limit(rps)
		ls.Lock()
		defer ls.Unlock()
		ls.limiters[method] = rate.NewLimiter(limit, rps)
	}

	return true
}

func (ls *methodLimiter) getMethod(method jsonrpc.RPCMethod) (*rate.Limiter, bool) {
	ls.RLock()
	defer ls.RUnlock()
	limiter, exists := ls.limiters[method]
	return limiter, exists
}

func (ls *methodLimiter) getAllLimits() map[jsonrpc.RPCMethod]int {
	ls.RLock()
	defer ls.RUnlock()

	all := make(map[jsonrpc.RPCMethod]int)
	for m, l := range ls.limiters {
		all[m] = int(l.Limit())
	}
	return all
}

func (ls *methodLimiter) Allow(method jsonrpc.RPCMethod) (allowed bool) {
	limiter, exist := ls.getMethod(method)
	if !exist {
		return true
	}
	return limiter.Allow()
}

const JRPCRateLimitHttpPath = "/jrpc/rate-limit"

func (ls *methodLimiter) RouterRegister(r *chi.Mux) {
	r.Handle(JRPCRateLimitHttpPath, ls.rateLimitUpdateHandler())
}

func (ls *methodLimiter) rateLimitUpdateHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		case http.MethodGet:
			all := ls.getAllLimits()
			var body bytes.Buffer
			err := json.ConfigStd.NewEncoder(&body).Encode(&all)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.WriteHeader(200)
			w.Write(body.Bytes())
		case http.MethodPut:
			payload := make(map[string]int)
			err := json.ConfigStd.NewDecoder(r.Body).Decode(&payload)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			for method, rps := range payload {
				updated := ls.UpdateLimit(jsonrpc.RPCMethod(method), rps)
				log.Info("Limiter update request",
					log.Any("rpc_method", method),
					log.Any("rate/s", rps),
					log.Any("updated", updated),
				)
			}

			w.WriteHeader(200)
		}
	})
}
