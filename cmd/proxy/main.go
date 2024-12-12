package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"

	"github.com/Chaintable/nodex-proxy/config"
	"github.com/Chaintable/nodex-proxy/discovery/etcd"
	"github.com/Chaintable/nodex-proxy/lb"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
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
	log.Info("config: %", zap.Any("config", config))
	log.ProductionModeWithoutStackTrace()

	var nodeRefresherMap = make(map[string]*etcd.Discover)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nodeRefresher, err := etcd.New(ctx, config.EtcdEndpoints, config.ProxyConfig.EtcdPrefix)
	if err != nil {
		log.Fatal("New refresher failed: %v\n", err)
	}
	defer nodeRefresher.Close()
	nodeChannel, heightChan, err := nodeRefresher.Init(ctx)
	if err != nil {
		log.Fatal("Init node refresher failed: %v\n", err)
	}

	limiter := jsonrpc.NewMethodLimiter(config.ProxyConfig.Processor.RateLimiter.RpcMethods)
	lb := lb.NewLoadBalancer(
		ctx,
		nodeRefresherMap, *config.ProxyConfig,
		&jsonrpc.GeneralRPCMethodHandler{Config: config.ProxyConfig}, limiter, nodeChannel, heightChan)
	go lb.BackgroundRefreshNode()
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
		log.Fatal("listen failed: %v\n", err)
	}
	server.Serve(listener)

	// sig := <-sigChan

	for _, refresher := range nodeRefresherMap {
		refresher.Close()
	}

	// log.Printf("[main] sig %v received, shutting down...", sig)
}
