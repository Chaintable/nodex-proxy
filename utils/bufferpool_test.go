package utils

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBufferPool(t *testing.T) {
	p := NewBufferPool()
	b1 := p.Get()
	p.Put(b1)

	// same object
	b2 := p.Get()
	b1[0] = uint8('1')
	assert.Equal(t, uint8('1'), b2[0])
	b2[1] = uint8('2')
	assert.Equal(t, uint8('2'), b1[1])
}

type testServers struct {
	backend        *httptest.Server
	frontend       *httptest.Server
	frontendClient *http.Client
	url            string
}

func setupForBenchmarkBufferPool(pool httputil.BufferPool) *testServers {
	m := http.NewServeMux()
	m.HandleFunc("/test", func(rw http.ResponseWriter, req *http.Request) {
		fmt.Fprint(rw, strings.Repeat("hello", 10240))
	})
	backend := httptest.NewServer(m)

	backendURL, _ := url.Parse(backend.URL)
	proxyHandler := httputil.NewSingleHostReverseProxy(backendURL)
	proxyHandler.BufferPool = pool
	proxyHandler.ErrorLog = log.New(ioutil.Discard, "", 0) // quiet for tests
	frontend := httptest.NewServer(proxyHandler)

	frontendClient := frontend.Client()
	frontendClient.Transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   1000,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	u := fmt.Sprintf("%s/test", frontend.URL)
	return &testServers{
		backend,
		frontend,
		frontendClient,
		u}
}

func BenchmarkBufferPool_enable(b *testing.B) {
	p := NewBufferPool()
	s := setupForBenchmarkBufferPool(p)
	frontendClient := s.frontendClient
	u := s.url
	defer s.backend.Close()
	defer s.frontend.Close()

	b.ResetTimer()
	b.StartTimer()
	defer b.StopTimer()

	for n := 0; n < b.N; n++ {
		resp, err := frontendClient.Get(u)
		if err != nil {
			fmt.Println(err)
		} else {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}
	}
}

func BenchmarkBufferPool_disable(b *testing.B) {
	s := setupForBenchmarkBufferPool(nil)
	frontendClient := s.frontendClient
	u := s.url
	defer s.backend.Close()
	defer s.frontend.Close()

	b.ResetTimer()
	b.StartTimer()
	defer b.StopTimer()

	for n := 0; n < b.N; n++ {
		resp, err := frontendClient.Get(u)
		if err != nil {
			fmt.Println(err)
		} else {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}
	}
}
