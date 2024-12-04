package lb

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/Chaintable/nodex-proxy/node"
	"github.com/Chaintable/nodex-proxy/utils"
	"golang.org/x/exp/rand"
)

type LoadBalancer struct {
	nodeRefresherMap map[string]*node.Refresher
	BufferPool       httputil.BufferPool
}

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
	if !strings.HasPrefix(urlStr, "http://") && !strings.HasPrefix(urlStr, "https://") {
		urlStr = "http://" + urlStr
	}
	url, err := url.Parse(urlStr)
	if err != nil {
		log.Printf("Failed to parse backend URL %s: %v, chain id: %+v", urlStr, err, chainID)
		http.Error(w, "Backend URL Parser ERR", http.StatusServiceUnavailable)
	}

	reverseProxy := httputil.NewSingleHostReverseProxy(url)
	reverseProxy.BufferPool = lb.BufferPool

	reverseProxy.ServeHTTP(w, r)
}
