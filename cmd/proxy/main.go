package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/Chaintable/nodex-proxy/config"
	"github.com/Chaintable/nodex-proxy/discovery/etcd"
	"github.com/Chaintable/nodex-proxy/lb"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/go-chi/chi/v5"
	"github.com/hertz-contrib/pprof"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"net"
	"net/http"
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
	heightMap := jsonrpc.NewHeightMap()
	lb := lb.NewLoadBalancer(
		ctx,
		nodeRefresherMap, *config.ProxyConfig,
		&jsonrpc.GeneralRPCMethodHandler{Config: config.ProxyConfig, HeightMap: heightMap},
		&jsonrpc.GeneralRPCMethodHertzHandler{Config: config.ProxyConfig, HeightMap: heightMap},
		limiter, heightMap, nodeChannel, heightChan)
	go lb.BackgroundRefreshNode()

	go func() {
		router := chi.NewRouter()
		router.Handle("/metrics", promhttp.InstrumentMetricHandler(
			prometheus.DefaultRegisterer, promhttp.HandlerFor(
				prometheus.DefaultGatherer,
				promhttp.HandlerOpts{MaxRequestsInFlight: 1024},
			),
		))
		mServer := http.Server{
			Handler: router,
		}
		// 启动服务器
		listener, err := net.Listen("tcp", fmt.Sprintf(":%s", config.MetricListen))
		if err != nil {
			log.Fatal("listen failed: %v\n", err)
		}
		err = mServer.Serve(listener)
		if err != nil {
			return
		}
	}()

	h := server.Default(server.WithHostPorts(fmt.Sprintf("0.0.0.0:%s", config.Listen)))

	pprof.Register(h)

	h.Any("/:chainId", func(ctx context.Context, c *app.RequestContext) {
		chainId := c.Param("chainId")
		lb.ServeHTTP(ctx, c, chainId)
	})
	h.Spin()

	for _, refresher := range nodeRefresherMap {
		refresher.Close()
	}

	// log.Printf("[main] sig %v received, shutting down...", sig)
}
