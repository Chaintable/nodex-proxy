package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/Chaintable/nodex-proxy/config"
	"github.com/Chaintable/nodex-proxy/discovery/etcd"
	"github.com/Chaintable/nodex-proxy/http_handler"
	"github.com/Chaintable/nodex-proxy/lb"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/Chaintable/nodex-proxy/usage"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/go-chi/chi/v5"
	"github.com/hertz-contrib/pprof"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

const (
	maxUpstreamAttempts   = 3
	shutdownRequestBuffer = 5 * time.Second
	maxExitWait           = time.Duration(1<<63 - 1)
)

type rpcRequestHandler interface {
	ServeHTTP(context.Context, *app.RequestContext, string)
}

type usageRecorder interface {
	Record(string, int64, time.Duration)
}

func newRPCRequestHandler(handler rpcRequestHandler, recorder usageRecorder) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		chainID := c.Param("chainId")
		if recorder == nil {
			handler.ServeHTTP(ctx, c, chainID)
			return
		}

		start := time.Now()
		clientID := c.Request.Header.Get("client-id")
		baseChainID, collectUsage := lb.ParseBaseChainID(chainID)
		defer func() {
			if collectUsage {
				recorder.Record(clientID, baseChainID, time.Since(start))
			}
		}()
		handler.ServeHTTP(ctx, c, chainID)
	}
}

func gracefulExitWait(proxyConfig *types.Config) time.Duration {
	if proxyConfig == nil {
		return shutdownRequestBuffer
	}
	maxTimeoutMS := proxyConfig.DefaultRPCTimeout
	for _, timeoutMS := range proxyConfig.RPCMethodTimeoutConfig {
		if timeoutMS > maxTimeoutMS {
			maxTimeoutMS = timeoutMS
		}
	}
	if maxTimeoutMS <= 0 {
		return shutdownRequestBuffer
	}
	maxPerAttemptMS := int64((maxExitWait - shutdownRequestBuffer) / time.Millisecond / maxUpstreamAttempts)
	perAttemptMS := int64(maxTimeoutMS)
	for _, overheadMS := range []int{proxyConfig.ConnDialTimeout, proxyConfig.ConnMaxWaitTimeout} {
		if overheadMS <= 0 {
			continue
		}
		if int64(overheadMS) > maxPerAttemptMS-perAttemptMS {
			return maxExitWait
		}
		perAttemptMS += int64(overheadMS)
	}
	if perAttemptMS > maxPerAttemptMS {
		return maxExitWait
	}
	return time.Duration(maxUpstreamAttempts*perAttemptMS)*time.Millisecond + shutdownRequestBuffer
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

func main() {
	cmdlineAndLoadConfig := parseCmdlineAndLoadConfig()
	log.InitLogger(cmdlineAndLoadConfig.LogLevel)

	log.Info("cmdlineAndLoadConfig: %", zap.Any("cmdlineAndLoadConfig", cmdlineAndLoadConfig))

	var usageCollector *usage.Collector
	if len(cmdlineAndLoadConfig.Usage.KafkaBrokers) > 0 {
		usageProducer, err := usage.NewKafkaProducer(cmdlineAndLoadConfig.Usage.KafkaBrokers)
		if err != nil {
			log.Fatal("failed to initialize usage Kafka producer", err)
		}
		usageCollector = usage.NewCollector(usageProducer)
		log.Info("usage collection enabled", log.Any("topic", usage.Topic))
	}

	nodeRefresherMap := make(map[string]*etcd.Discover)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nodeRefresher, err := etcd.New(ctx, cmdlineAndLoadConfig.EtcdEndpoints, cmdlineAndLoadConfig.ProxyConfig.EtcdPrefix)
	if err != nil {
		log.Fatal("New refresher failed: %v\n", err)
	}
	defer nodeRefresher.Close()
	nodeChannel, heightChan, gatewayChannel, mirrorChannel, versionChannel, err := nodeRefresher.Init(ctx)
	if err != nil {
		log.Fatal("Init node refresher failed: %v\n", err)
	}

	limiter := jsonrpc.NewMethodLimiter(cmdlineAndLoadConfig.ProxyConfig.Processor.RateLimiter.RpcMethods)
	heightMap := jsonrpc.NewHeightMap()
	mirrorLimiter := jsonrpc.NewMirrorLimiter()
	loadBalancer := lb.NewLoadBalancer(
		ctx,
		nodeRefresherMap, *cmdlineAndLoadConfig.ProxyConfig,
		&jsonrpc.GeneralRPCMethodHertzHandler{Config: cmdlineAndLoadConfig.ProxyConfig, HeightMap: heightMap},
		limiter, heightMap, mirrorLimiter, nodeChannel, heightChan, gatewayChannel, mirrorChannel, versionChannel)
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

	h := server.Default(
		server.WithHostPorts(fmt.Sprintf("0.0.0.0:%s", cmdlineAndLoadConfig.Listen)),
		server.WithExitWaitTime(gracefulExitWait(cmdlineAndLoadConfig.ProxyConfig)),
	)

	pprof.Register(h)

	handler, err := http_handler.NewHandler(ctx, loadBalancer.NodeSelector, cmdlineAndLoadConfig.EtcdEndpoints, cmdlineAndLoadConfig.ProxyConfig.EtcdPrefix, nodeChannel, loadBalancer)
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
	h.POST("/:chainId/addLocalNode", handler.AddLocalNode)
	h.DELETE("/:chainId/deleteLocalNode/:nodeKey", handler.DeleteLocalNode)

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

	// Local mirror management endpoints (memory only)
	h.POST("/:chainId/addLocalMirror", handler.AddLocalMirror)
	h.DELETE("/:chainId/deleteLocalMirror", handler.DeleteLocalMirror)
	h.DELETE("/:chainId/deleteAllLocalMirrors", handler.DeleteAllLocalMirrors)

	// Persistent mirror management endpoints (etcd)
	h.POST("/:chainId/addMirror", handler.AddMirror)
	h.DELETE("/:chainId/deleteMirror", handler.DeleteMirror)
	h.DELETE("/:chainId/deleteAllMirrors", handler.DeleteAllMirrors)

	// Mirror query endpoints (query from memory)
	h.GET("/:chainId/getMirrors", handler.GetMirrors)
	h.GET("/getAllMirrors", handler.GetAllMirrors)

	var recorder usageRecorder
	if usageCollector != nil {
		recorder = usageCollector
	}
	h.Any("/:chainId", newRPCRequestHandler(loadBalancer, recorder))

	h.Spin()

	if usageCollector != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := usageCollector.Close(shutdownCtx); err != nil {
			log.Error("failed to flush usage records during shutdown", err)
		}
		shutdownCancel()
	}

	for _, refresher := range nodeRefresherMap {
		refresher.Close()
	}
}
