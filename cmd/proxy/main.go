package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"

	"github.com/Chaintable/nodex-proxy/config"
	"github.com/Chaintable/nodex-proxy/discovery/etcd"
	"github.com/Chaintable/nodex-proxy/http_handler"
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
	log.InitLogger(cmdlineAndLoadConfig.LogLevel)

	log.Info("cmdlineAndLoadConfig: %", zap.Any("cmdlineAndLoadConfig", cmdlineAndLoadConfig))
	//log.DevelopmentMode()

	var nodeRefresherMap = make(map[string]*etcd.Discover)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nodeRefresher, err := etcd.New(ctx, cmdlineAndLoadConfig.EtcdEndpoints, cmdlineAndLoadConfig.ProxyConfig.EtcdPrefix)
	if err != nil {
		log.Fatal("New refresher failed: %v\n", err)
	}
	defer nodeRefresher.Close()
	nodeChannel, heightChan, gatewayChannel, mirrorChannel, err := nodeRefresher.Init(ctx)
	if err != nil {
		log.Fatal("Init node refresher failed: %v\n", err)
	}

	limiter := jsonrpc.NewMethodLimiter(cmdlineAndLoadConfig.ProxyConfig.Processor.RateLimiter.RpcMethods)
	heightMap := jsonrpc.NewHeightMap()
	loadBalancer := lb.NewLoadBalancer(
		ctx,
		nodeRefresherMap, *cmdlineAndLoadConfig.ProxyConfig,
		&jsonrpc.GeneralRPCMethodHertzHandler{Config: cmdlineAndLoadConfig.ProxyConfig, HeightMap: heightMap},
		limiter, heightMap, nodeChannel, heightChan, gatewayChannel, mirrorChannel)
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

	handler, err := http_handler.NewHandler(ctx, loadBalancer.NodeSelector, cmdlineAndLoadConfig.EtcdEndpoints, cmdlineAndLoadConfig.ProxyConfig.EtcdPrefix, loadBalancer)
	if err != nil {
		log.Fatal("New handler failed: %v\n", err)
	}

	// Add weight management endpoints
	h.GET("/getChains", handler.GetAllChainsIDs)
	h.POST("/:chainId/setWeight", handler.SetWeight)
	h.GET("/:chainId/getWeight", handler.GetWeight)
	h.DELETE("/:chainId/deleteWeight", handler.DeleteWeight)

	h.GET("/:chainId/getAllNodes", handler.GetAllNodes)
	h.GET("/:chainId/debug_chooseOneNode", handler.ChooseOneNode)
	h.POST("/:chainId/addNode", handler.AddNode)
	h.DELETE("/:chainId/deleteNode/:nodeKey", handler.DeleteNode)
	h.PUT("/:chainId/updateNode/:nodeKey", handler.UpdateNode)

	// Add method route management endpoints
	h.POST("/:chainId/addMethodRoute", handler.AddMethodRoute)
	h.POST("/:chainId/removeMethodRoute", handler.RemoveMethodRoute)
	h.DELETE("/:chainId/deleteMethodRoute/:method", handler.DeleteMethodRoute)
	h.GET("/:chainId/getAllMethodRoutes", handler.GetMethodRoutes)
	h.GET("/:chainId/getMethodRoute/:method", handler.GetMethodRoute)

	// Add writer management endpoints
	h.GET("/:chainId/writers", handler.GetWriters)
	h.POST("/:chainId/writers/switchLeader", handler.SwitchLeader)
	h.GET("/:chainId/writers/leader", handler.GetLeaderStatus)

	// Add mirror management endpoints
	h.POST("/:chainId/addMirror", handler.AddMirror)
	h.DELETE("/:chainId/deleteMirror", handler.DeleteMirror)
	h.DELETE("/:chainId/deleteAllMirrors", handler.DeleteAllMirrors)
	h.GET("/:chainId/getMirrors", handler.GetMirrors)
	h.GET("/getAllMirrors", handler.GetAllMirrors)

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
