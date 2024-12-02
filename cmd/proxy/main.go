package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/Chaintable/nodex-proxy/config"
	"github.com/Chaintable/nodex-proxy/lb"
	"github.com/Chaintable/nodex-proxy/node"
	"github.com/gin-gonic/gin"
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
		nodeRefresher := node.NewRefresher(replicaNotificationSetting.EtcdEndpoints, replicaNotificationSetting.Key, replicaNotificationSetting.ChainID)
		nodeRefresherMap[replicaNotificationSetting.ChainID] = nodeRefresher
	}

	lb := lb.NewLoadBalancer(nodeRefresherMap)

	router := gin.Default()

	// 定义带有 path 参数的路由
	router.GET("/:chain_id", func(c *gin.Context) {
		chainID := c.Param("chain_id")
		rw := c.Writer
		req := c.Request

		lb.ServeHTTP(rw, req, chainID)
	})

	router.POST("/:chain_id", func(c *gin.Context) {
		chainID := c.Param("chain_id")
		rw := c.Writer
		req := c.Request

		lb.ServeHTTP(rw, req, chainID)
	})

	// 启动服务器
	router.Run(fmt.Sprintf(":%s", config.Listen))

	// sig := <-sigChan

	for _, refresher := range nodeRefresherMap {
		refresher.Close()
	}

	// log.Printf("[main] sig %v received, shutting down...", sig)
}
