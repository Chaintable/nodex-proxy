package lb

import (
	"net/http"
	"net/http/httputil"

	"github.com/Chaintable/nodex-proxy/node"
	"github.com/Chaintable/nodex-proxy/utils"
	"golang.org/x/exp/rand"
)

type LoadBalancer struct {
	nodeRefresherMap map[string]*node.Refresher
	BufferPool       httputil.BufferPool
}

var headerUserAgent = "User-Agent"

func NewLoadBalancer(nodeRefresherMap map[string]*node.Refresher) *LoadBalancer {
	return &LoadBalancer{nodeRefresherMap: nodeRefresherMap, BufferPool: utils.NewBufferPool()}
}

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request, chainID string) {
	if lb.nodeRefresherMap[chainID] == nil {
		http.Error(w, "No backends available", http.StatusBadGateway)
	}
	backends := lb.nodeRefresherMap[chainID].GetBackends()
	if len(backends) == 0 {
		http.Error(w, "No backends available", http.StatusServiceUnavailable)
		return
	}
	urlStr := backends[rand.Intn(len(backends))]

	reverseProxy := &httputil.ReverseProxy{
		Director:   lb.forwardDirector(urlStr, r),
		BufferPool: lb.BufferPool,
	}

	reverseProxy.ServeHTTP(w, r)
}

func (lb *LoadBalancer) forwardDirector(host string, inReq *http.Request) func(*http.Request) {
	return func(outReq *http.Request) {
		outReq.URL = cloneURL(outReq.URL)
		outReq.URL.Scheme = "http"
		outReq.URL.Host = host
		outReq.URL.Path = inReq.URL.Path
		outReq.URL.RawPath = inReq.URL.RawPath
		outReq.URL.RawQuery = inReq.URL.RawQuery
		outReq.RequestURI = ""
		outReq.Host = inReq.Host
		if outReq.Host == "" {
			outReq.Host = inReq.URL.Host
		}

		if _, ok := outReq.Header[headerUserAgent]; !ok {
			outReq.Header.Set(headerUserAgent, "")
		}
	}
}
