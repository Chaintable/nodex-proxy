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
	"fmt"
	"net/http"
	"time"
)

const (
	ProxyServeTimeHeader = "X-Serve-Time"
)

// LoggingDuration records the time cost to HEADER during request starts and response end.
func LoggingDuration(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&timedResponseWriter{
			ResponseWriter: w,
			start:          time.Now(),
			headerWrote:    false,
		}, r)
	}
	return http.HandlerFunc(fn)
}

// timedResponseWriter a helper object that inject `X-Serve-Time` Header to underlying response writer.
type timedResponseWriter struct {
	http.ResponseWriter
	start       time.Time
	headerWrote bool
}

func (w *timedResponseWriter) Write(b []byte) (int, error) {
	if !w.headerWrote {
		w.WriteHeader(200)
	}

	return w.ResponseWriter.Write(b)
}

func (w *timedResponseWriter) WriteHeader(statusCode int) {
	elapsed := time.Since(w.start).Milliseconds()
	w.Header().Set(ProxyServeTimeHeader, fmt.Sprintf("%dms", elapsed))
	w.ResponseWriter.WriteHeader(statusCode)
	w.headerWrote = true
}
