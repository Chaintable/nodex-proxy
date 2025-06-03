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
	"github.com/Chaintable/nodex-proxy/lb/weight"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/go-chi/chi/v5"
	"github.com/hertz-contrib/pprof"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	cmdlineAndLoadConfig := parseCmdlineAndLoadConfig()
	log.Info("cmdlineAndLoadConfig: %", zap.Any("cmdlineAndLoadConfig", cmdlineAndLoadConfig))
	log.ProductionModeWithoutStackTrace()

	var nodeRefresherMap = make(map[string]*etcd.Discover)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nodeRefresher, err := etcd.New(ctx, cmdlineAndLoadConfig.EtcdEndpoints, cmdlineAndLoadConfig.ProxyConfig.EtcdPrefix)
	if err != nil {
		log.Fatal("New refresher failed: %v\n", err)
	}
	defer nodeRefresher.Close()
	nodeChannel, heightChan, err := nodeRefresher.Init(ctx)
	if err != nil {
		log.Fatal("Init node refresher failed: %v\n", err)
	}

	limiter := jsonrpc.NewMethodLimiter(cmdlineAndLoadConfig.ProxyConfig.Processor.RateLimiter.RpcMethods)
	heightMap := jsonrpc.NewHeightMap()
	loadBalancer := lb.NewLoadBalancer(
		ctx,
		nodeRefresherMap, *cmdlineAndLoadConfig.ProxyConfig,
		&jsonrpc.GeneralRPCMethodHertzHandler{Config: cmdlineAndLoadConfig.ProxyConfig, HeightMap: heightMap},
		limiter, heightMap, nodeChannel, heightChan)
	go loadBalancer.BackgroundRefreshNode()

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
		listener, err := net.Listen("tcp", fmt.Sprintf(":%s", cmdlineAndLoadConfig.MetricListen))
		if err != nil {
			log.Fatal("listen failed: %v\n", err)
		}
		err = mServer.Serve(listener)
		if err != nil {
			return
		}
	}()

	h := server.Default(server.WithHostPorts(fmt.Sprintf("0.0.0.0:%s", cmdlineAndLoadConfig.Listen)))

	pprof.Register(h)

	// Initialize weight handler
	weightHandler := weight.NewHandler(loadBalancer.NodeSelector)

	// Add weight management endpoints
	h.POST("/:chainId/setWeight", weightHandler.SetWeight)
	h.GET("/:chainId/getWeight", weightHandler.GetWeight)
	h.DELETE("/:chainId/deleteWeight", weightHandler.DeleteWeight)
	h.GET("/:chainId/getAllNodes", weightHandler.GetAllNodes)
	h.GET("/:chainId/debug_chooseOneNode", weightHandler.ChooseOneNode)

	h.Any("/:chainId", func(ctx context.Context, c *app.RequestContext) {
		chainId := c.Param("chainId")
		loadBalancer.ServeHTTP(ctx, c, chainId)
	})

	h.Spin()

	for _, refresher := range nodeRefresherMap {
		refresher.Close()
	}

	// log.Printf("[main] sig %v received, shutting down...", sig)
}
