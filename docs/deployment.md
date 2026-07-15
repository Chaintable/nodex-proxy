# Deployment Guide

English | [中文](deployment_cn.md)

This guide covers deploying nodex-proxy in various environments: bare-metal, Docker, Docker Compose, and Kubernetes.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Deployment Options](#deployment-options)
  - [1. Bare-Metal (Build from Source)](#1-bare-metal-build-from-source)
  - [2. Docker](#2-docker)
  - [3. Docker Compose](#3-docker-compose)
  - [4. Kubernetes](#4-kubernetes)
- [Port Reference](#port-reference)
- [Monitoring](#monitoring)
- [etcd Service Discovery](#etcd-service-discovery)
- [Production Tuning](#production-tuning)

## Prerequisites

| Component | Version | Required |
|-----------|---------|----------|
| Go | 1.22+ | Bare-metal only |
| Docker | 20.10+ | Docker / Compose / K8s |
| docker-compose | v2+ | Compose only |
| kubectl | 1.24+ | K8s only |
| etcd | v3.5+ | All deployments |

## Configuration

nodex-proxy reads a YAML configuration file at startup. See [config/config.example.yaml](../config/config.example.yaml) for a full example.

### Top-Level Launch Configuration

The launch configuration is passed via CLI flags or the outer YAML wrapper:

```yaml
listen: "8663"                 # RPC server listen port
metric_listen: "8664"          # Prometheus metrics port
etcd_endpoints:                # etcd cluster endpoints
  - "http://etcd1:2379"
  - "http://etcd2:2379"
  - "http://etcd3:2379"
log_level: "info"              # debug, info, warn, error
usage:                         # optional usage reporting
  kafka_brokers:               # omit or leave empty to disable
    - "kafka-1:9092"
proxy_config:                  # proxy configuration (see config.example.yaml)
  ...
```

When `usage.kafka_brokers` is non-empty, RPC duration is aggregated in memory
by `client-id` and base chain ID, then written every 30 seconds to the fixed
`leafage-usage` topic. Missing or blank client IDs are reported as `unknown`.
Usage delivery is best-effort; graceful shutdown sends the final batch, while
process crashes and Kafka failures may lose data. The topic must already exist.
Client IDs over 256 bytes and new aggregation keys beyond 100,000 per window
are discarded to bound memory use; see `jrpcx_usage_discarded_requests_total`.

### CLI Flags

| Flag | Description | Priority |
|------|-------------|----------|
| `-config` | Path to YAML configuration file | Required |
| `-listen` | Override RPC listen port (e.g. `8663`) | Overrides config file |

## Deployment Options

### 1. Bare-Metal (Build from Source)

#### Build

```bash
git clone https://github.com/Chaintable/nodex-proxy.git
cd nodex-proxy
go build -o node-proxy cmd/proxy/main.go
```

#### Run

```bash
./node-proxy -config config/config.example.yaml -listen 8663
```

#### Systemd Service (Production)

Create `/etc/systemd/system/nodex-proxy.service`:

```ini
[Unit]
Description=nodex-proxy JSON-RPC Proxy
After=network.target etcd.service
Wants=network-online.target

[Service]
Type=simple
User=nodex
Group=nodex
WorkingDirectory=/opt/nodex-proxy
ExecStart=/opt/nodex-proxy/node-proxy -config /opt/nodex-proxy/config.yaml -listen 8663
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/nodex-proxy

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable nodex-proxy
sudo systemctl start nodex-proxy
sudo systemctl status nodex-proxy
```

View logs:

```bash
journalctl -u nodex-proxy -f
```

### 2. Docker

#### Build Image

The Dockerfile uses a two-stage build (golang:1.23-bookworm → ubuntu:24.04):

```bash
docker build -t nodex-proxy:latest .
```

#### Run Container

```bash
docker run -d \
  --name nodex-proxy \
  --restart unless-stopped \
  -p 8663:8663 \
  -p 8664:8664 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  nodex-proxy:latest \
  -config /app/config.yaml
```

> **Note:** Make sure the `etcd_endpoints` in your config.yaml are reachable from within the container. Use `host.docker.internal` or a Docker network to connect to etcd.

### 3. Docker Compose

Create a `docker-compose.yaml` in the project root with nodex-proxy and a 3-node etcd cluster:

```yaml
version: "3.8"

services:
  etcd1:
    image: quay.io/coreos/etcd:v3.5.17
    command:
      - etcd
      - --name=etcd1
      - --data-dir=/etcd-data
      - --listen-client-urls=http://0.0.0.0:2379
      - --advertise-client-urls=http://etcd1:2379
      - --listen-peer-urls=http://0.0.0.0:2380
      - --initial-advertise-peer-urls=http://etcd1:2380
      - --initial-cluster=etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380
      - --initial-cluster-state=new
    volumes:
      - etcd1-data:/etcd-data
    networks:
      - nodex-net

  etcd2:
    image: quay.io/coreos/etcd:v3.5.17
    command:
      - etcd
      - --name=etcd2
      - --data-dir=/etcd-data
      - --listen-client-urls=http://0.0.0.0:2379
      - --advertise-client-urls=http://etcd2:2379
      - --listen-peer-urls=http://0.0.0.0:2380
      - --initial-advertise-peer-urls=http://etcd2:2380
      - --initial-cluster=etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380
      - --initial-cluster-state=new
    volumes:
      - etcd2-data:/etcd-data
    networks:
      - nodex-net

  etcd3:
    image: quay.io/coreos/etcd:v3.5.17
    command:
      - etcd
      - --name=etcd3
      - --data-dir=/etcd-data
      - --listen-client-urls=http://0.0.0.0:2379
      - --advertise-client-urls=http://etcd3:2379
      - --listen-peer-urls=http://0.0.0.0:2380
      - --initial-advertise-peer-urls=http://etcd3:2380
      - --initial-cluster=etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380
      - --initial-cluster-state=new
    volumes:
      - etcd3-data:/etcd-data
    networks:
      - nodex-net

  nodex-proxy:
    build:
      context: .
    # Or use a pre-built image:
    # image: nodex-proxy:latest
    ports:
      - "8663:8663"
      - "8664:8664"
    volumes:
      - ./config.yaml:/app/config.yaml:ro
    command: ["-config", "/app/config.yaml"]
    depends_on:
      - etcd1
      - etcd2
      - etcd3
    restart: unless-stopped
    networks:
      - nodex-net

volumes:
  etcd1-data:
  etcd2-data:
  etcd3-data:

networks:
  nodex-net:
    driver: bridge
```

The corresponding `config.yaml` should reference the etcd services by name:

```yaml
listen: "8663"
metric_listen: "8664"
etcd_endpoints:
  - "http://etcd1:2379"
  - "http://etcd2:2379"
  - "http://etcd3:2379"
log_level: "info"
proxy_config:
  service_name: "jrpcx"
  # ... (see config/config.example.yaml for full options)
```

Start:

```bash
docker compose up -d
```

Stop:

```bash
docker compose down
```

### 4. Kubernetes

#### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: nodex-proxy-config
  namespace: default
data:
  config.yaml: |
    listen: "8663"
    metric_listen: "8664"
    etcd_endpoints:
      - "http://etcd-0.etcd-headless:2379"
      - "http://etcd-1.etcd-headless:2379"
      - "http://etcd-2.etcd-headless:2379"
    log_level: "info"
    proxy_config:
      service_name: "jrpcx"
      default_rpc_timeout: 5000
      connection_pool_size: 2000
      node_select_strategy: "random"
      processor:
        observability_log:
          enable: true
          enable_error_log: true
          slow_threshold:
            default: 500
        method_denied:
          - eth_newPendingTransactionFilter
          - txpool_content
          - txpool_inspect
          - txpool_contentFrom
          - txpool_status
      observability:
        trace:
          enable: false
        log:
          sampling:
            enable: true
            initial: 100
            thereafter: 100
        static_resource:
          service.name: "nodex-proxy"
          service.version: "1.0.0"
```

#### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nodex-proxy
  namespace: default
  labels:
    app: nodex-proxy
spec:
  replicas: 3
  selector:
    matchLabels:
      app: nodex-proxy
  template:
    metadata:
      labels:
        app: nodex-proxy
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8664"
        prometheus.io/path: "/metrics"
    spec:
      containers:
        - name: nodex-proxy
          image: nodex-proxy:latest
          args: ["-config", "/etc/nodex-proxy/config.yaml"]
          ports:
            - name: rpc
              containerPort: 8663
              protocol: TCP
            - name: metrics
              containerPort: 8664
              protocol: TCP
          resources:
            requests:
              cpu: "500m"
              memory: "256Mi"
            limits:
              cpu: "2000m"
              memory: "1Gi"
          livenessProbe:
            httpGet:
              path: /metrics
              port: metrics
            initialDelaySeconds: 10
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /metrics
              port: metrics
            initialDelaySeconds: 5
            periodSeconds: 10
          volumeMounts:
            - name: config
              mountPath: /etc/nodex-proxy
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: nodex-proxy-config
```

#### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: nodex-proxy
  namespace: default
  labels:
    app: nodex-proxy
spec:
  type: ClusterIP
  ports:
    - name: rpc
      port: 8663
      targetPort: rpc
      protocol: TCP
    - name: metrics
      port: 8664
      targetPort: metrics
      protocol: TCP
  selector:
    app: nodex-proxy
```

#### HorizontalPodAutoscaler

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: nodex-proxy
  namespace: default
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: nodex-proxy
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80
```

Apply all resources:

```bash
kubectl apply -f configmap.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl apply -f hpa.yaml
```

## Port Reference

| Port | Protocol | Description | Configuration Key |
|------|----------|-------------|-------------------|
| 8663 | TCP | JSON-RPC server (main) | `listen` or `-listen` flag |
| 8664 | TCP | Prometheus metrics | `metric_listen` |

## Monitoring

### Prometheus

Metrics are exposed at `http://<host>:8664/metrics`. Add a scrape target to your Prometheus configuration:

```yaml
scrape_configs:
  - job_name: "nodex-proxy"
    scrape_interval: 15s
    static_configs:
      - targets: ["nodex-proxy:8664"]
```

For Kubernetes, the Deployment template includes Prometheus annotations for auto-discovery.

### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `jrpcx_rpc_calls_started` | Counter | RPC calls initiated |
| `jrpcx_rpc_calls_finished` | Counter | RPC calls completed |
| `jrpcx_rpc_calls_failed` | Counter | Failed RPC calls |
| `jrpcx_rpc_calls_time` | Histogram | RPC latency in milliseconds |
| `jrpcx_rpc_calls_cache_hits` | Counter | Cache hit count |
| `jrpcx_rpc_request_payload_sizes` | Histogram | Request payload size |
| `jrpcx_rpc_response_payload_sizes` | Histogram | Response payload size |
| `jrpcx_rpc_batch_calls_finished` | Counter | Batch request count |
| `jrpcx_rpc_batch_calls_time` | Histogram | Batch request latency |
| `jrpcx_rpc_http_status_code` | Counter | HTTP status codes |

Common labels: `host`, `target`, `chain_id`, `chain_version`. Method metrics add `method`; `jrpcx_rpc_calls_started` also adds `sourcedapp`; failures add `status_code`, `upstream_related`, and `reason`.

### Grafana

Import the Prometheus data source and create dashboards using the metrics above. Recommended panels:

- **RPC QPS**: `rate(jrpcx_rpc_calls_finished[5m])`
- **Error Rate**: `rate(jrpcx_rpc_calls_failed[5m]) / rate(jrpcx_rpc_calls_finished[5m])`
- **P99 Latency**: `histogram_quantile(0.99, rate(jrpcx_rpc_calls_time_bucket[5m]))`
- **Cache Hit Rate**: `rate(jrpcx_rpc_calls_cache_hits[5m]) / rate(jrpcx_rpc_calls_finished[5m])`

## etcd Service Discovery

nodex-proxy uses etcd to dynamically discover blockchain nodes and configuration. The proxy watches for `PUT` and `DELETE` events to add/remove nodes in real time.

### Key Structure

All keys use the configured `etcd_prefix`:

| Key Pattern | Description |
|-------------|-------------|
| `{prefix}/{chainId}/nodes/{nodeKey}` | State/Archive nodes |
| `{prefix}/{chainId}/nativeNodes/{nodeKey}` | Native fallback nodes |
| `{prefix}/{chainId}/lastBlockNumber` | Current chain block height |
| `{prefix}/{chainId}/gateway` | Gateway config (weights, method routes) |
| `{prefix}/{chainId}/mirror/{addrKey}` | Mirror targets |
| `{prefix}/{chainId}/version` | Chain version info |
| `{prefix}/{chainId}/{version}/nodes/{nodeKey}` | Versioned nodes |

### Register a Node

Use `etcdctl` to register a node:

```bash
# Register a state node for chain 1
etcdctl put "/myprefix/1/nodes/node1" '{"url":"http://geth-node1:8545","weight":100}'

# Register a native fallback node
etcdctl put "/myprefix/1/nativeNodes/native1" '{"url":"http://native-node1:8545"}'

# Set current block height
etcdctl put "/myprefix/1/lastBlockNumber" "20000000"
```

### Node Health Check

When a new node is discovered via etcd:

1. An `eth_blockNumber` RPC call is sent to verify the node is reachable
2. On failure, retries every 5 seconds (configurable via `node_health_check_timeout`)
3. Max wait time: 300 seconds (configurable via `node_health_check_max_wait`)
4. Only after a successful health check is the node added to the load balancer pool

## Production Tuning

### Connection Pool

```yaml
proxy_config:
  connection_pool_size: 2000    # Adjust based on expected concurrent connections
```

For high-traffic deployments, increase this value. Monitor connection-related errors and adjust accordingly.

### Timeouts

```yaml
proxy_config:
  default_rpc_timeout: 5000       # Default timeout for RPC calls (ms)
  node_health_check_timeout: 5    # Health check retry interval (seconds)
  node_health_check_max_wait: 300 # Max wait for node to become healthy (seconds)
```

### Rate Limiting

Configure per-method rate limits to protect backend nodes:

```yaml
proxy_config:
  processor:
    rate_limiter:
      rpc_methods:
        eth_call: 100          # requests per second
        eth_getLogs: 50
        eth_getBlockByNumber: 200
```

### Log Sampling

For high-traffic environments, enable log sampling to reduce log volume:

```yaml
proxy_config:
  observability:
    log:
      sampling:
        enable: true
        initial: 100         # Log first N entries per second
        thereafter: 100      # Then log every Nth entry
```

### Slow Request Thresholds

```yaml
proxy_config:
  processor:
    observability_log:
      enable: true
      enable_error_log: true
      slow_threshold:
        default: 500         # Default slow threshold (ms)
        rpc_methods:
          eth_call: 1000     # Per-method override
          eth_getLogs: 2000
```

### System Limits (Bare-Metal)

For high-traffic production deployments, increase system file descriptor limits:

```bash
# /etc/security/limits.conf
nodex    soft    nofile    65535
nodex    hard    nofile    65535
```
