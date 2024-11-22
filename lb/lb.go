package lb

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/Chaintable/nodex-proxy/node"
	"golang.org/x/exp/rand"
)

type LoadBalancer struct {
	nodeRefresher *node.Refresher
}

func NewLoadBalancer(nodeRefresher *node.Refresher) *LoadBalancer {
	return &LoadBalancer{nodeRefresher: nodeRefresher}
}

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	backends := lb.nodeRefresher.GetBackends()
	if len(backends) == 0 {
		http.Error(w, "No backends available", http.StatusServiceUnavailable)
		return
	}
	urlStr := backends[rand.Intn(len(backends))]

	url, err := url.Parse(urlStr)
	if err != nil {
		log.Printf("Failed to parse backend URL %s: %v", urlStr, err)
		http.Error(w, "Backend URL Parser ERR", http.StatusServiceUnavailable)
	}

	reverseProxy := httputil.NewSingleHostReverseProxy(url)

	reverseProxy.ServeHTTP(w, r)
}
