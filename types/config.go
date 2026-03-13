// Copyright (c) 2022 DeBank Inc. <admin@debank.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package types

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"github.com/Chaintable/nodex-proxy/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/go-chi/chi/v5"
	"go.uber.org/atomic"
	"gopkg.in/yaml.v3"
)

var (
	DefaultWeight                 = 100
	DefaultLoadBalancerTargetPort = 80
)

// defaultConfig default configuration if config file not specified.
var defaultConfig = Config{
	ServiceName:            "jrpcx",
	RPCListen:              ":9545",
	InternalAPIListen:      ":9268",
	EnablePProf:            false,
	NativeNodeURL:          "http://127.0.0.1:8545",
	OfficialNodeURL:        "",
	BlockReaderCacheTTL:    1,
	DefaultRPCTimeout:      5000,
	ConnectionPoolSize:     2000,
	RPCMethodTimeoutConfig: map[string]int{},
	Processor: ProcessorConfig{
		ObservabilityLog: ObservabilityLogProcessorConfig{
			Enable: false,
			SlowThreshold: SlowLogThresholdConfig{
				Default:    500,
				RpcMethods: map[jsonrpc.RPCMethod]int{},
			},
		},
		RateLimiter: RateLimiterProcessorConfig{
			RpcMethods: map[string]int{},
		},
		BlockRangeQueryLimit: BlockRangeQueryLimitProcessorConfig{
			Enable:          false,
			RecentBlocks:    0,
			RewriteToLatest: false,
		},
		RequestMirror: RequestMirrorConfig{
			Enable: false,
		},
		MethodNameChecker: MethodNameCheckerConfig{
			Enable: false,
			Regexp: "^[a-zA-Z_][a-zA-Z0-9_]*$", // by default only alphanumeric and "_" are allowed, numeric cannot be leading character
		},
		MethodDenied: &[]jsonrpc.RPCMethod{
			"eth_newPendingTransactionFilter",
			"txpool_content",
			"txpool_inspect",
			"txpool_contentFrom",
			"txpool_status",
		},
	},
	Observability: ObservabilityConfig{
		Trace: ObservabilityTraceConfig{
			SamplingRatio: 0.1,
		},
		Log: log.ObservabilityLogConfig{
			Sampling: &log.ObservabilityLogSamplingConfig{
				Enable:     true,
				Initial:    100,
				Thereafter: 100,
			},
		},
	},
	NodeSelectStrategy:     "random",
	EtcdPrefix:             "",
	NodeHealthCheckTimeout: 5,   // in seconds
	NodeHealthCheckMaxWait: 300, // in seconds
}

type ObservabilityLogProcessorConfig struct {
	Enable         bool                   `yaml:"enable"`
	SlowThreshold  SlowLogThresholdConfig `yaml:"slow_threshold"`
	EnableErrorLog bool                   `yaml:"enable_error_log"`
}

// SlowLogThresholdConfig configuration of slow log threshold, in millisecond.
type SlowLogThresholdConfig struct {
	Default    int                       `yaml:"default"`
	RpcMethods map[jsonrpc.RPCMethod]int `yaml:"rpc_methods"`
}

type RateLimiterProcessorConfig struct {
	RpcMethods map[string]int `yaml:"rpc_methods"` // map[method]rps
}

type BlockRangeQueryLimitProcessorConfig struct {
	Enable          bool   `yaml:"enable"`
	HashBlockEnable bool   `yaml:"hash_block_enable"`
	RecentBlocks    uint64 `yaml:"recent_blocks"` // most recently blocks
	RewriteToLatest bool   `yaml:"rewrite_to_latest"`
}

type RequestMirrorConfig struct {
	Enable bool `yaml:"enable"`
}

type MethodNameCheckerConfig struct {
	Enable       bool   `yaml:"enable"`
	Regexp       string `yaml:"regexp"`
	DeniedRegexp string `yaml:"denied_regexp"`
}

type ProcessorConfig struct {
	ObservabilityLog     ObservabilityLogProcessorConfig     `yaml:"observability_log"`
	RateLimiter          RateLimiterProcessorConfig          `yaml:"rate_limiter"`
	BlockRangeQueryLimit BlockRangeQueryLimitProcessorConfig `yaml:"block_range_query_limit"`
	RequestMirror        RequestMirrorConfig                 `yaml:"request_mirror"`
	MethodNameChecker    MethodNameCheckerConfig             `yaml:"method_name_checker"`
	MethodDenied         *[]jsonrpc.RPCMethod                `yaml:"method_denied"`
}

type ObservabilityTraceConfig struct {
	Enable        bool    `yaml:"enable"`
	OTLPEndpoint  string  `yaml:"otlp_endpoint"`
	SamplingRatio float64 `yaml:"sampling_ratio"`
}
type (
	ObservabilityMetricConfig struct{}
	ObservabilityConfig       struct {
		Trace  ObservabilityTraceConfig   `yaml:"trace"`
		Metric ObservabilityMetricConfig  `yaml:"metric"`
		Log    log.ObservabilityLogConfig `yaml:"log"`
		// StaticResource can contain for example information about the application that emits the record or about the infrastructure where the application runs.
		StaticResource map[string]string `yaml:"static_resource"`
	}
)

// Config ...
type Config struct {
	ServiceName            string              `yaml:"service_name"`
	RPCListen              string              `yaml:"rpc_listen"`
	InternalAPIListen      string              `yaml:"internal_api_listen"`
	EnablePProf            bool                `yaml:"enable_pprof"`
	NativeNodeURL          string              `yaml:"native_node_url"`
	ReverseProxy           bool                `yaml:"reverse_proxy"`
	DefaultRPCTimeout      int                 `yaml:"default_rpc_timeout"`
	RPCMethodTimeoutConfig map[string]int      `yaml:"rpc_method_timeout_config"`
	OfficialNodeURL        string              `yaml:"official_node_url"`
	BlockReaderCacheTTL    int                 `yaml:"block_reader_cache_ttl"`
	ChainId                string              `yaml:"chain_id"`
	ConnectionPoolSize     int                 `yaml:"connection_pool_size"`
	Processor              ProcessorConfig     `yaml:"processor"`
	Observability          ObservabilityConfig `yaml:"observability"`
	NodeSelectStrategy     string              `yaml:"node_select_strategy"`
	EtcdPrefix             string              `yaml:"etcd_prefix"`
	NodeHealthCheckTimeout int                 `yaml:"node_health_check_timeout"`  // in seconds
	NodeHealthCheckMaxWait int                 `yaml:"node_health_check_max_wait"` // in seconds
}

type RaftJoinConfig struct {
	// Id identity of the node to be joined
	Id                       uint64
	Host                     string
	NativeBlockChainNodeHost string
}

type RaftConfig struct {
	// Id identity of current raft node.
	Id         uint64
	Join       bool
	JoinConfig RaftJoinConfig

	SnapRootDir string `yaml:"snapRootDir"`
	WALDir      string `yaml:"walDir"`
	SnapDir     string `yaml:"snapDir"`
	SnapCount   uint64 `yaml:"snapCount"`

	MinBlockNumberGap uint64 `yaml:"minBlockNumberGap"`

	Host string `yaml:"host"`
}

// NewConfig creates a new config object.
func NewConfig(filepath string, chainId string) (cfg Config, err error) {
	if filepath == "" {
		defaultConfig.ChainId = chainId
		cfg.initMetrics()
		return defaultConfig, nil
	}
	bs, err := os.ReadFile(filepath)
	if err != nil {
		return
	}

	cfg.ChainId = chainId
	if err = yaml.Unmarshal(bs, &cfg); err != nil {
		return
	}

	FillWithDefaultConfig(&cfg)
	return
}

func FillWithDefaultConfig(cfg *Config) {
	if cfg.ServiceName == "" {
		cfg.ServiceName = defaultConfig.ServiceName
	}
	if cfg.RPCListen == "" {
		cfg.RPCListen = defaultConfig.RPCListen
	}
	if cfg.InternalAPIListen == "" {
		cfg.InternalAPIListen = defaultConfig.InternalAPIListen
	}
	if cfg.NativeNodeURL == "" {
		cfg.NativeNodeURL = defaultConfig.NativeNodeURL
	}
	if cfg.BlockReaderCacheTTL == 0 {
		cfg.BlockReaderCacheTTL = defaultConfig.BlockReaderCacheTTL
	}
	if cfg.DefaultRPCTimeout == 0 {
		cfg.DefaultRPCTimeout = defaultConfig.DefaultRPCTimeout
	}
	if cfg.ConnectionPoolSize == 0 {
		cfg.ConnectionPoolSize = defaultConfig.ConnectionPoolSize
	}
	if cfg.RPCMethodTimeoutConfig == nil {
		cfg.RPCMethodTimeoutConfig = map[string]int{}
	}
	// processor config

	// Observability Log Processor
	defaultObservabilityLogProcessConfig := defaultConfig.Processor.ObservabilityLog
	observabilityLogProcessConfig := &cfg.Processor.ObservabilityLog
	if observabilityLogProcessConfig.SlowThreshold.Default == 0 {
		observabilityLogProcessConfig.SlowThreshold.Default = defaultObservabilityLogProcessConfig.SlowThreshold.Default
	}
	if observabilityLogProcessConfig.SlowThreshold.RpcMethods == nil { // make sure RpcMethods isn't nil value for avoid panic reason.
		observabilityLogProcessConfig.SlowThreshold.RpcMethods = defaultObservabilityLogProcessConfig.SlowThreshold.RpcMethods
	}

	defaultMethodNameCheckerProcessConfig := defaultConfig.Processor.MethodNameChecker
	methodNameProcessConfig := &cfg.Processor.MethodNameChecker
	if methodNameProcessConfig.Regexp == "" {
		methodNameProcessConfig.Regexp = defaultMethodNameCheckerProcessConfig.Regexp
	}

	if cfg.Processor.MethodDenied == nil {
		cfg.Processor.MethodDenied = defaultConfig.Processor.MethodDenied
	}

	// rate limiter processor config
	defaultRateLimiterProcessConfig := defaultConfig.Processor.RateLimiter
	rateLimiterProcessConfig := &cfg.Processor.RateLimiter
	if rateLimiterProcessConfig.RpcMethods == nil {
		rateLimiterProcessConfig.RpcMethods = defaultRateLimiterProcessConfig.RpcMethods
	}

	// raft processor config
	// trace
	defaultObservabilityTraceConfig := defaultConfig.Observability.Trace
	observabilityTraceConfig := &cfg.Observability.Trace
	if observabilityTraceConfig.SamplingRatio == 0 {
		observabilityTraceConfig.SamplingRatio = defaultObservabilityTraceConfig.SamplingRatio
	}

	// log
	defaultObservabilityLogConfig := defaultConfig.Observability.Log
	observabilityLogConfig := &cfg.Observability.Log
	if observabilityLogConfig.Sampling == nil {
		observabilityLogConfig.Sampling = defaultObservabilityLogConfig.Sampling
	}
	if cfg.NodeSelectStrategy == "" {
		cfg.NodeSelectStrategy = defaultConfig.NodeSelectStrategy
	}
	if cfg.EtcdPrefix == "" {
		cfg.EtcdPrefix = defaultConfig.EtcdPrefix
	}
	cfg.initMetrics()
}

type debugInfo struct {
	mode *atomic.Bool
}

var DebugInfo = &debugInfo{
	mode: atomic.NewBool(false),
}

const (
	DebugHTTPPath = "/status/jrpc-debug"
)

func (d *debugInfo) RouterRegister(r *chi.Mux) {
	r.Handle(DebugHTTPPath, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.Method {
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		case http.MethodGet:
			status := d.mode.Load()
			if status {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case http.MethodPut:
			log.Info("Debug mode enabled")
			d.mode.Store(true)
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			log.Info("Debug mode disabled")
			d.mode.Store(false)
			w.WriteHeader(http.StatusOK)
		}
	}))
}

func (d *debugInfo) DebugModeEnable() bool {
	return d.mode.Load()
}

const (
	MirrorRequestHost = "mirror"
)

var jrpcxFileHome string

func init() {
	home, e := os.UserHomeDir()
	if e != nil {
		panic(e)
	}
	jrpcxFileHome = filepath.Join(home, ".jrpcx")
	_, err := os.Stat(jrpcxFileHome)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.Mkdir(jrpcxFileHome, 0o777)
			if err != nil && !errors.Is(err, os.ErrExist) {
				panic(err)
			}
		} else {
			panic(err)
		}
	}
}

type jrpcxFilePath string

func (rawP jrpcxFilePath) AbsFilePath() (string, error) {
	p, e := filepath.Abs(filepath.Join(jrpcxFileHome, string(rawP)))
	if e != nil {
		return "", e
	}
	return p, nil
}

func (rawP jrpcxFilePath) FileCreate() error {
	p, err := ManualProbeFlagFilePath.AbsFilePath()
	if err != nil {
		return err
	}
	f, err := os.Create(p)
	if err != nil {
		return err
	}
	return f.Close()
}

func (rawP jrpcxFilePath) FileExist() bool {
	p, err := ManualProbeFlagFilePath.AbsFilePath()
	if err != nil {
		return false
	}
	_, err = os.Stat(p)
	return err == nil
}

func (rawP jrpcxFilePath) FileDelete() error {
	p, err := ManualProbeFlagFilePath.AbsFilePath()
	if err != nil {
		return err
	}
	return os.Remove(p)
}

type manualProbeFlagType struct{}

var ManualProbeFlag = manualProbeFlagType{}

const (
	ManualProbeFlagFilePath jrpcxFilePath = "jrpcx-manual-probe-flag"
	ManualProbeFlagHTTPPath string        = "/jrpc/manual-probe-flag"
)

func (_ manualProbeFlagType) RouterRegister(r *chi.Mux) {
	r.Handle(ManualProbeFlagHTTPPath, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			case http.MethodPut:
				if err := ManualProbeFlagFilePath.FileCreate(); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				w.WriteHeader(http.StatusCreated)
			case http.MethodDelete:
				if err := ManualProbeFlagFilePath.FileDelete(); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				w.WriteHeader(http.StatusOK)
			}
		}))
}

func DefaultConfig() Config {
	return defaultConfig
}

func (c Config) InternalAPIHost() string {
	host, port, err := net.SplitHostPort(c.InternalAPIListen)
	if err != nil {
		log.Fatal("SplitHostPort error", err)
	}
	if host == "" {
		host = "0.0.0.0"
	}
	return fmt.Sprintf("http://%s:%s", host, port)
}
