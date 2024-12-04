package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/Chaintable/nodex-proxy/config"
	"github.com/Chaintable/nodex-proxy/lb"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc"
	"github.com/Chaintable/nodex-proxy/node"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/go-chi/chi/v5"
)

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

func main() {
	config := parseCmdlineAndLoadConfig()
	log.Printf("config: %+v", config)

	var nodeRefresherMap = make(map[string]*node.Refresher)

	for _, replicaNotificationSetting := range config.ReplicaNotificationSettings {
		nodeRefresher, err := node.NewRefresher(replicaNotificationSetting.EtcdEndpoints, replicaNotificationSetting.Key, replicaNotificationSetting.ChainID)
		if err != nil {
			log.Fatalf("new refresher failed: %v\n", err)
		}
		nodeRefresherMap[replicaNotificationSetting.ChainID] = nodeRefresher
	}
	pConfig := types.DefaultConfig()
	limiter := jsonrpc.NewMethodLimiter(pConfig.Processor.RateLimiter.RpcMethods)
	lb := lb.NewLoadBalancer(nodeRefresherMap, pConfig, &jsonrpc.GeneralRPCMethodHandler{Config: &pConfig}, limiter)

	router := chi.NewRouter()
	router.HandleFunc("/{chainId}", func(rw http.ResponseWriter, r *http.Request) {
		chainId := chi.URLParam(r, "chainId")
		lb.ServeHTTP(rw, r, chainId)
	})
	server := http.Server{
		Handler: router,
	}
	// 启动服务器
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", config.Listen))
	if err != nil {
		log.Fatalf("listen failed: %v\n", err)
	}
	server.Serve(listener)

	// sig := <-sigChan

	for _, refresher := range nodeRefresherMap {
		refresher.Close()
	}

	// log.Printf("[main] sig %v received, shutting down...", sig)
}
