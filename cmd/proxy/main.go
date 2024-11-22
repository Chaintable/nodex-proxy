package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Chaintable/nodex-proxy/config"
	"github.com/Chaintable/nodex-proxy/lb"
	"github.com/Chaintable/nodex-proxy/node"
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
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	config := parseCmdlineAndLoadConfig()
	log.Printf("[main] config: %+v", config)

	nodeRefresher := node.NewRefresher(config.InnerReplicaBrokers, config.InnerReplicaStateChangeTopic, "")

	go func() {
		nodeRefresher.Refresh()
	}()

	lb := lb.NewLoadBalancer(nodeRefresher)

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
