# nodex-proxy

English | [中文](README_CN.md)

A high-performance blockchain JSON-RPC proxy server with load balancing, health checking, rate limiting, and observability. It sits between clients and blockchain nodes, distributing requests across a pool of nodes while providing advanced traffic management capabilities.

## Features

- **Multi-Chain Support** — Manage multiple blockchain chains simultaneously with per-chain configuration
- **Load Balancing** — Random (with weighted gateway support) and Round-Robin strategies
- **Node Health Checking** — Automatic health verification with configurable timeouts and retries before adding nodes to the pool
- **Rate Limiting** — Per-method RPC request rate limiting (token bucket algorithm)
- **Request Mirroring** — Duplicate requests to mirror targets for analysis/monitoring
- **Method Routing** — Route specific RPC methods to specific node subsets based on inclusion/exclusion rules
- **Block Range Query Limiting** — Restrict block range queries to prevent excessive resource consumption
- **Method Validation** — Regex-based validation and deny list for RPC method names
- **Observability** — OpenTelemetry tracing, Prometheus metrics, structured logging with sampling
- **Service Discovery** — etcd-based dynamic node registration and configuration
- **Version Routing** — Support for versioned chain endpoints for multi-version deployments
- **Batch Request Support** — Handle JSON-RPC batch requests with unified timeout

## Architecture

```
                    ┌──────────────────┐
                    │   HTTP Clients   │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  Hertz HTTP      │
                    │  Server (:8663)  │
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
        ┌──────────┐  ┌───────────┐  ┌───────────┐
        │  Pre-    │  │ Processor │  │  Post-    │
        │Processor │  │ Pipeline  │  │Processor  │
        │(Validate)│  │ (Handle)  │  │ (Log)     │
        └────┬─────┘  └───────────┘  └───────────┘
             │
             ▼
        ┌───────────────────────────────────┐
        │         Load Balancer             │
        │  ┌─────────────────────────────┐  │
        │  │  Node Selector Strategy     │  │
        │  │  (Random / Round-Robin)     │  │
        │  ├─────────────────────────────┤  │
        │  │  Gateway (Weights/Routes)   │  │
        │  └─────────────────────────────┘  │
        └───────────────┬───────────────────┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
        ▼               ▼               ▼
   ┌─────────┐    ┌──────────┐   ┌──────────┐
   │ Archive │    │  State   │   │  Native  │
   │  Nodes  │    │  Nodes   │   │  Nodes   │
   └─────────┘    └──────────┘   └──────────┘

   etcd ──── Service Discovery & Config ────►
   Prometheus ◄──── Metrics (:8664) ────
```

For component boundaries, request lifecycle, dynamic configuration, and operational design details, see the [Design Architecture](docs/architecture.md).

**Node selection logic:**

- `latest` / `pending` block → State nodes
- Within 64 blocks of head → State nodes
- More than 64 blocks behind → Archive nodes
- Explicit native request → Native nodes

## Quick Start

> For comprehensive deployment instructions (Docker Compose, Kubernetes, systemd, production tuning), see the [Deployment Guide](docs/deployment.md).

### Prerequisites

- Go 1.22+
- etcd v3 cluster (for service discovery)

### Build

```bash
go build -o node-proxy cmd/proxy/main.go
```

### Docker

```bash
docker build -t nodex-proxy:latest .
docker run -p 8663:8663 -p 8664:8664 nodex-proxy:latest -config /path/to/config.yaml
```

### Run

```bash
./node-proxy -config config/config.example.yaml -listen 8663
```

**Flags:**

| Flag | Description |
|------|-------------|
| `-config` | Path to YAML configuration file |
| `-listen` | Override RPC listen port (takes precedence over config) |

**Ports:**

| Port | Description |
|------|-------------|
| `8663` | RPC server (configurable via `-listen`) |
| `8664` | Prometheus metrics endpoint |

## Configuration

See [config/config.example.yaml](config/config.example.yaml) for a full example.

### Core Settings

```yaml
listen: "8663"
metric_listen: "8664"
etcd_endpoints:
  - "http://127.0.0.1:2379"
log_level: "info"

# Optional; an empty list disables usage reporting.
usage:
  kafka_brokers:
    - "kafka-1:9092"

proxy_config:
  service_name: "jrpcx"
  native_node_url: "http://127.0.0.1:8545"
  default_rpc_timeout: 5000            # milliseconds
  connection_pool_size: 2000
  node_select_strategy: "random"       # "random" or "round_robin"
  etcd_prefix: ""
```

When enabled, RPC duration is aggregated in memory by `client-id` and base
chain ID and written to the fixed `leafage-usage` Kafka topic every 30 seconds.
A missing or blank `client-id` is reported as `unknown`. Delivery is
best-effort: the final in-memory batch is sent during graceful shutdown, but
data can be lost on process crashes or Kafka failures. To bound memory exposed
to untrusted headers, client IDs over 256 bytes and new aggregation keys beyond
100,000 in one window are discarded and counted in Prometheus metrics.

### Processor Pipeline

```yaml
proxy_config:
  processor:
    # Slow request logging
    observability_log:
      enable: true
      enable_error_log: true
      slow_threshold:
        default: 500               # ms
        rpc_methods:
          eth_call: 1000
          eth_getLogs: 2000

    # Per-method rate limiting (requests per second)
    rate_limiter:
      rpc_methods:
        eth_call: 100
        eth_getLogs: 50

    # Block range query restriction
    block_range_query_limit:
      enable: false
      recent_blocks: 0
      rewrite_to_latest: false

    # Request mirroring
    request_mirror:
      enable: false

    # Method name validation
    method_name_checker:
      enable: false
      regexp: "^[a-zA-Z_][a-zA-Z0-9_]*$"

    # Denied RPC methods
    method_denied:
      - eth_newPendingTransactionFilter
      - txpool_content
      - txpool_inspect
      - txpool_contentFrom
      - txpool_status
```

### Observability

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

## Service Discovery (etcd)

Nodes and configuration are managed through etcd with the following key structure:

| Key Pattern | Description |
|-------------|-------------|
| `{prefix}/{chainId}/nodes/{nodeKey}` | State/Archive nodes |
| `{prefix}/{chainId}/nativeNodes/{nodeKey}` | Native fallback nodes |
| `{prefix}/{chainId}/lastBlockNumber` | Current chain block height |
| `{prefix}/{chainId}/gateway` | Gateway config (weights, method routes) |
| `{prefix}/{chainId}/mirror/{addrKey}` | Mirror targets |
| `{prefix}/{chainId}/version` | Chain version info |
| `{prefix}/{chainId}/{version}/nodes/{nodeKey}` | Versioned nodes |

The proxy watches for etcd `PUT` and `DELETE` events to dynamically add/remove nodes and update configurations at runtime.

## Health Checking

When a new node is discovered via etcd:

1. An `eth_blockNumber` RPC call is sent to verify the node is reachable
2. On failure, retries every **5 seconds** up to a configurable max wait time (default: **300s**)
3. Only after a successful health check is the node added to the load balancer pool

## Metrics

Prometheus metrics are exposed at `http://<host>:8664/metrics`.

| Metric | Type | Description |
|--------|------|-------------|
| `jrpcx_rpc_calls_started` | Counter | RPC calls initiated |
| `jrpcx_rpc_calls_finished` | Counter | RPC calls completed |
| `jrpcx_rpc_calls_failed` | Counter | Failed RPC calls |
| `jrpcx_rpc_calls_time` | Histogram | RPC latency (ms) |
| `jrpcx_rpc_calls_cache_hits` | Counter | Cache hit count |
| `jrpcx_rpc_request_payload_sizes` | Histogram | Request payload size |
| `jrpcx_rpc_response_payload_sizes` | Histogram | Response payload size |
| `jrpcx_rpc_batch_calls_finished` | Counter | Batch request count |
| `jrpcx_rpc_batch_calls_time` | Histogram | Batch request latency |
| `jrpcx_rpc_http_status_code` | Counter | HTTP status codes |

Common labels include `host`, `target`, `chain_id`, and `chain_version`. Method metrics add `method`; `jrpcx_rpc_calls_started` also adds `sourcedapp`; failures add `status_code`, `upstream_related`, and `reason`.

## Key Dependencies

| Dependency | Purpose |
|------------|---------|
| [cloudwego/hertz](https://github.com/cloudwego/hertz) | HTTP framework |
| [ethereum/go-ethereum](https://github.com/ethereum/go-ethereum) | Ethereum types & RPC |
| [etcd/client/v3](https://github.com/etcd-io/etcd) | Service discovery |
| [opentelemetry](https://opentelemetry.io/) | Distributed tracing |
| [uber/zap](https://github.com/uber-go/zap) | Structured logging |
| [bytedance/sonic](https://github.com/bytedance/sonic) | High-performance JSON |
| [prometheus/client_golang](https://github.com/prometheus/client_golang) | Metrics |

## License

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
