# nodex-proxy

[English](README.md) | 中文

一个高性能的区块链 JSON-RPC 代理服务器，具备负载均衡、健康检查、限流和可观测性等功能。它位于客户端与区块链节点之间，将请求分发到节点池中，同时提供高级流量管理能力。

## 功能特性

- **多链支持** — 同时管理多条区块链，支持按链独立配置
- **负载均衡** — 支持随机（含加权网关）和轮询策略
- **节点健康检查** — 自动健康验证，可配置超时和重试次数，通过检查后才加入节点池
- **限流** — 基于令牌桶算法的按方法 RPC 请求限流
- **请求镜像** — 将请求复制到镜像目标用于分析/监控
- **方法路由** — 基于包含/排除规则将特定 RPC 方法路由到特定节点子集
- **区块范围查询限制** — 限制区块范围查询以防止资源过度消耗
- **方法校验** — 基于正则表达式的 RPC 方法名验证与拒绝列表
- **可观测性** — OpenTelemetry 链路追踪、Prometheus 指标、结构化日志与采样
- **服务发现** — 基于 etcd 的动态节点注册与配置
- **版本路由** — 支持版本化链端点，适用于多版本部署
- **批量请求支持** — 处理 JSON-RPC 批量请求，统一超时控制

## 架构

```
                    ┌──────────────────┐
                    │   HTTP 客户端    │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  Hertz HTTP      │
                    │  服务器 (:8663)  │
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
        ┌──────────┐  ┌───────────┐  ┌───────────┐
        │  预处理  │  │  处理流水 │  │  后处理   │
        │ (校验)   │  │  线(处理) │  │  (日志)   │
        └────┬─────┘  └───────────┘  └───────────┘
             │
             ▼
        ┌───────────────────────────────────┐
        │           负载均衡器              │
        │  ┌─────────────────────────────┐  │
        │  │   节点选择策略              │  │
        │  │  (随机 / 轮询)             │  │
        │  ├─────────────────────────────┤  │
        │  │  网关 (权重/路由)           │  │
        │  └─────────────────────────────┘  │
        └───────────────┬───────────────────┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
        ▼               ▼               ▼
   ┌─────────┐    ┌──────────┐   ┌──────────┐
   │  归档   │    │   状态   │   │   原生   │
   │  节点   │    │   节点   │   │   节点   │
   └─────────┘    └──────────┘   └──────────┘

   etcd ──── 服务发现 & 配置 ────►
   Prometheus ◄──── 指标 (:8664) ────
```

组件边界、请求生命周期、动态配置和运维设计细节请参考[设计架构](docs/architecture_cn.md)。

**节点选择逻辑：**

- `latest` / `pending` 区块 → 状态节点
- 距离最新区块 64 块以内 → 状态节点
- 落后最新区块超过 64 块 → 归档节点
- 显式原生请求 → 原生节点

## 快速开始

> 完整的部署说明（Docker Compose、Kubernetes、systemd、生产调优）请参阅[部署指南](docs/deployment_cn.md)。

### 前置条件

- Go 1.22+
- etcd v3 集群（用于服务发现）

### 构建

```bash
go build -o node-proxy cmd/proxy/main.go
```

### Docker

```bash
docker build -t nodex-proxy:latest .
docker run -p 8663:8663 -p 8664:8664 nodex-proxy:latest -config /path/to/config.yaml
```

### 运行

```bash
./node-proxy -config config/config.example.yaml -listen 8663
```

**命令行参数：**

| 参数 | 说明 |
|------|------|
| `-config` | YAML 配置文件路径 |
| `-listen` | 覆盖 RPC 监听端口（优先级高于配置文件） |

**端口：**

| 端口 | 说明 |
|------|------|
| `8663` | RPC 服务（可通过 `-listen` 配置） |
| `8664` | Prometheus 指标端点 |

## 配置

完整示例请参考 [config/config.example.yaml](config/config.example.yaml)。

### 核心配置

```yaml
listen: "8663"
metric_listen: "8664"
etcd_endpoints:
  - "http://127.0.0.1:2379"
log_level: "info"

# 可选；空列表表示关闭用量上报。
usage:
  kafka_brokers:
    - "kafka-1:9092"

proxy_config:
  service_name: "jrpcx"
  native_node_url: "http://127.0.0.1:8545"
  default_rpc_timeout: 5000            # 毫秒
  connection_pool_size: 2000
  node_select_strategy: "random"       # "random" 或 "round_robin"
  etcd_prefix: ""
```

开启后，RPC 请求耗时会在本地按 `client-id` 和基础 chain ID 聚合，达到 10,000 个聚合键
时立即写入固定的 Kafka Topic `leafage-usage`，最长刷出间隔为 30 秒。缺失或空白的
`client-id` 统一记为 `unknown`。`jrpcx_usage_aggregation_keys` 指标记录当前内存中的
聚合键数量，包括正在写入 Kafka 的批次。发送采用 best-effort 语义：优雅退出时会发送
最后一批内存数据，但进程崩溃或 Kafka 异常时允许丢失。

### 处理流水线

```yaml
proxy_config:
  processor:
    # 慢请求日志
    observability_log:
      enable: true
      enable_error_log: true
      slow_threshold:
        default: 500               # 毫秒
        rpc_methods:
          eth_call: 1000
          eth_getLogs: 2000

    # 按方法限流（每秒请求数）
    rate_limiter:
      rpc_methods:
        eth_call: 100
        eth_getLogs: 50

    # 区块范围查询限制
    block_range_query_limit:
      enable: false
      recent_blocks: 0
      rewrite_to_latest: false

    # 请求镜像
    request_mirror:
      enable: false

    # 方法名校验
    method_name_checker:
      enable: false
      regexp: "^[a-zA-Z_][a-zA-Z0-9_]*$"

    # 拒绝的 RPC 方法
    method_denied:
      - eth_newPendingTransactionFilter
      - txpool_content
      - txpool_inspect
      - txpool_contentFrom
      - txpool_status
```

### 可观测性

```yaml
proxy_config:
  observability:
    trace:
      enable: false
      otlp_endpoint: ""
      sampling_ratio: 0.1
    log:
      sampling:
        enable: true
        initial: 100
        thereafter: 100
    static_resource:
      service.name: "nodex-proxy"
      service.version: "1.0.0"
```

## 服务发现（etcd）

节点和配置通过 etcd 管理，键结构如下：

| 键模式 | 说明 |
|--------|------|
| `{prefix}/{chainId}/nodes/{nodeKey}` | 状态/归档节点 |
| `{prefix}/{chainId}/nativeNodes/{nodeKey}` | 原生回退节点 |
| `{prefix}/{chainId}/lastBlockNumber` | 当前链区块高度 |
| `{prefix}/{chainId}/gateway` | 网关配置（权重、方法路由） |
| `{prefix}/{chainId}/mirror/{addrKey}` | 镜像目标 |
| `{prefix}/{chainId}/version` | 链版本信息 |
| `{prefix}/{chainId}/{version}/nodes/{nodeKey}` | 版本化节点 |

代理通过监听 etcd 的 `PUT` 和 `DELETE` 事件来动态添加/移除节点并在运行时更新配置。

## 健康检查

当通过 etcd 发现新节点时：

1. 发送 `eth_blockNumber` RPC 调用以验证节点可达性
2. 如果失败，每 **5 秒** 重试一次，最多等待可配置的最大时间（默认：**300 秒**）
3. 只有通过健康检查后，节点才会被加入负载均衡池

## 指标

Prometheus 指标暴露在 `http://<host>:8664/metrics`。

| 指标 | 类型 | 说明 |
|------|------|------|
| `jrpcx_rpc_calls_started` | Counter | 已发起的 RPC 调用数 |
| `jrpcx_rpc_calls_finished` | Counter | 已完成的 RPC 调用数 |
| `jrpcx_rpc_calls_failed` | Counter | 失败的 RPC 调用数 |
| `jrpcx_rpc_calls_time` | Histogram | RPC 延迟（毫秒） |
| `jrpcx_rpc_calls_cache_hits` | Counter | 缓存命中次数 |
| `jrpcx_rpc_request_payload_sizes` | Histogram | 请求负载大小 |
| `jrpcx_rpc_response_payload_sizes` | Histogram | 响应负载大小 |
| `jrpcx_rpc_batch_calls_finished` | Counter | 批量请求数 |
| `jrpcx_rpc_batch_calls_time` | Histogram | 批量请求延迟 |
| `jrpcx_rpc_http_status_code` | Counter | HTTP 状态码 |

通用标签包括 `host`、`target`、`chain_id` 和 `chain_version`。按方法统计的指标会增加 `method`；`jrpcx_rpc_calls_started` 还会增加 `sourcedapp`；失败指标会增加 `status_code`、`upstream_related` 和 `reason`。

## 主要依赖

| 依赖 | 用途 |
|------|------|
| [cloudwego/hertz](https://github.com/cloudwego/hertz) | HTTP 框架 |
| [ethereum/go-ethereum](https://github.com/ethereum/go-ethereum) | 以太坊类型 & RPC |
| [etcd/client/v3](https://github.com/etcd-io/etcd) | 服务发现 |
| [opentelemetry](https://opentelemetry.io/) | 分布式链路追踪 |
| [uber/zap](https://github.com/uber-go/zap) | 结构化日志 |
| [bytedance/sonic](https://github.com/bytedance/sonic) | 高性能 JSON |
| [prometheus/client_golang](https://github.com/prometheus/client_golang) | 指标采集 |

## 许可证

MIT License

Copyright (c) 2022 DeBank Inc. <admin@debank.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
