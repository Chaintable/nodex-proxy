package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/Chaintable/nodex-proxy/config"
	"github.com/Chaintable/nodex-proxy/node"
)

type Backend struct {
	URL          *url.URL
	ReverseProxy *httputil.ReverseProxy
}

type LoadBalancer struct {
	nodeRefresher *node.Refresher
}

func parseCmdlineAndLoadConfig() config.Config {
	cmdlineConfig := config.Config{}

	flag.StringVar(&cmdlineConfig.Listen, "listen", "", "listen")

	configFilePath := flag.String("config", "", "config file")

	flag.Parse()

	// load file config
	fileConfig := config.LoadConfig(*configFilePath)

	// override file config with cmdline config
	if cmdlineConfig.Listen != "" {
		fileConfig.Listen = cmdlineConfig.Listen
	}

	return fileConfig
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

func main() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	config := parseCmdlineAndLoadConfig()
	log.Printf("[main] config: %+v", config)

	nodeRefresher := node.NewRefresher(config.InnerReplicaBrokers, config.InnerReplicaStateChangeTopic, config.InnerReplicaGroupID)

	go func() {
		nodeRefresher.Refresh()
	}()

	lb := NewLoadBalancer(nodeRefresher)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		lb.ServeHTTP(w, r)
	})

	log.Println("Starting load balancer on :", config.Listen)
	if err := http.ListenAndServe(fmt.Sprintf(":%s", config.Listen), nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	sig := <-sigChan

	nodeRefresher.Close()

	log.Printf("[main] sig %v received, shutting down...", sig)
}
