# 部署指南

[English](deployment.md) | 中文

本指南介绍如何在不同环境中部署 nodex-proxy：裸机、Docker、Docker Compose 和 Kubernetes。

## 目录

- [前置条件](#前置条件)
- [配置说明](#配置说明)
- [部署方式](#部署方式)
  - [1. 裸机部署（源码编译）](#1-裸机部署源码编译)
  - [2. Docker 部署](#2-docker-部署)
  - [3. Docker Compose 部署](#3-docker-compose-部署)
  - [4. Kubernetes 部署](#4-kubernetes-部署)
- [端口说明](#端口说明)
- [监控](#监控)
- [etcd 服务发现](#etcd-服务发现)
- [生产环境调优](#生产环境调优)

## 前置条件

| 组件 | 版本 | 适用场景 |
|------|------|----------|
| Go | 1.22+ | 仅裸机部署 |
| Docker | 20.10+ | Docker / Compose / K8s |
| docker-compose | v2+ | 仅 Compose 部署 |
| kubectl | 1.24+ | 仅 K8s 部署 |
| etcd | v3.5+ | 所有部署方式 |

## 配置说明

nodex-proxy 在启动时读取 YAML 配置文件。完整示例请参考 [config/config.example.yaml](../config/config.example.yaml)。

### 顶层启动配置

启动配置通过 CLI 参数或外层 YAML 传递：

```yaml
listen: "8663"                 # RPC 服务器监听端口
metric_listen: "8664"          # Prometheus 指标端口
etcd_endpoints:                # etcd 集群端点
  - "http://etcd1:2379"
  - "http://etcd2:2379"
  - "http://etcd3:2379"
log_level: "info"              # debug, info, warn, error
usage:                         # 可选的用量上报配置
  kafka_brokers:               # 不配置或留空表示关闭
    - "kafka-1:9092"
proxy_config:                  # 代理配置（详见 config.example.yaml）
  ...
```

当 `usage.kafka_brokers` 非空时，RPC 请求耗时会在内存中按 `client-id` 和基础 chain ID
聚合，达到 10,000 个聚合键时立即写入固定的 `leafage-usage` Topic，最长刷出间隔为
30 秒。缺失或空白的客户端 ID 记为 `unknown`。`jrpcx_usage_aggregation_keys` 指标记录
当前内存聚合键数量，包括正在写入 Kafka 的批次。用量发送采用 best-effort 语义：优雅退出时会发送最后一批数据，但
进程崩溃或 Kafka 异常时允许丢失。Topic 需要提前创建。

### CLI 参数

| 参数 | 说明 | 优先级 |
|------|------|--------|
| `-config` | YAML 配置文件路径 | 必需 |
| `-listen` | 覆盖 RPC 监听端口（如 `8663`） | 优先于配置文件 |

## 部署方式

### 1. 裸机部署（源码编译）

#### 编译

```bash
git clone https://github.com/Chaintable/nodex-proxy.git
cd nodex-proxy
go build -o node-proxy cmd/proxy/main.go
```

#### 运行

```bash
./node-proxy -config config/config.example.yaml -listen 8663
```

#### Systemd 服务（生产环境）

创建 `/etc/systemd/system/nodex-proxy.service`：

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

# 安全加固
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/nodex-proxy

[Install]
WantedBy=multi-user.target
```

启用并启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable nodex-proxy
sudo systemctl start nodex-proxy
sudo systemctl status nodex-proxy
```

查看日志：

```bash
journalctl -u nodex-proxy -f
```

### 2. Docker 部署

#### 构建镜像

Dockerfile 使用两阶段构建（golang:1.23-bookworm → ubuntu:24.04）：

```bash
docker build -t nodex-proxy:latest .
```

#### 运行容器

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

> **注意：** 确保 config.yaml 中的 `etcd_endpoints` 在容器内可达。使用 `host.docker.internal` 或 Docker 网络连接 etcd。

### 3. Docker Compose 部署

在项目根目录创建 `docker-compose.yaml`，包含 nodex-proxy 和 3 节点 etcd 集群：

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
    # 或使用预构建镜像：
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

对应的 `config.yaml` 应使用服务名引用 etcd：

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
  # ...（完整选项请参考 config/config.example.yaml）
```

启动：

```bash
docker compose up -d
```

停止：

```bash
docker compose down
```

### 4. Kubernetes 部署

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

应用所有资源：

```bash
kubectl apply -f configmap.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl apply -f hpa.yaml
```

## 端口说明

| 端口 | 协议 | 说明 | 配置项 |
|------|------|------|--------|
| 8663 | TCP | JSON-RPC 服务器（主端口） | `listen` 或 `-listen` 参数 |
| 8664 | TCP | Prometheus 指标 | `metric_listen` |

## 监控

### Prometheus

指标通过 `http://<host>:8664/metrics` 暴露。在 Prometheus 配置中添加采集目标：

```yaml
scrape_configs:
  - job_name: "nodex-proxy"
    scrape_interval: 15s
    static_configs:
      - targets: ["nodex-proxy:8664"]
```

对于 Kubernetes 部署，Deployment 模板已包含 Prometheus 自动发现注解。

### 关键指标

| 指标 | 类型 | 说明 |
|------|------|------|
| `jrpcx_rpc_calls_started` | Counter | 已发起的 RPC 调用数 |
| `jrpcx_rpc_calls_finished` | Counter | 已完成的 RPC 调用数 |
| `jrpcx_rpc_calls_failed` | Counter | 失败的 RPC 调用数 |
| `jrpcx_rpc_calls_time` | Histogram | RPC 延迟（毫秒） |
| `jrpcx_rpc_calls_cache_hits` | Counter | 缓存命中数 |
| `jrpcx_rpc_request_payload_sizes` | Histogram | 请求负载大小 |
| `jrpcx_rpc_response_payload_sizes` | Histogram | 响应负载大小 |
| `jrpcx_rpc_batch_calls_finished` | Counter | 批量请求数 |
| `jrpcx_rpc_batch_calls_time` | Histogram | 批量请求延迟 |
| `jrpcx_rpc_http_status_code` | Counter | HTTP 状态码 |

通用标签：`host`、`target`、`chain_id`、`chain_version`。按方法统计的指标会增加 `method`；`jrpcx_rpc_calls_started` 还会增加 `sourcedapp`；失败指标会增加 `status_code`、`upstream_related` 和 `reason`。

### Grafana

导入 Prometheus 数据源并使用上述指标创建仪表板。推荐面板：

- **RPC QPS**：`rate(jrpcx_rpc_calls_finished[5m])`
- **错误率**：`rate(jrpcx_rpc_calls_failed[5m]) / rate(jrpcx_rpc_calls_finished[5m])`
- **P99 延迟**：`histogram_quantile(0.99, rate(jrpcx_rpc_calls_time_bucket[5m]))`
- **缓存命中率**：`rate(jrpcx_rpc_calls_cache_hits[5m]) / rate(jrpcx_rpc_calls_finished[5m])`

## etcd 服务发现

nodex-proxy 使用 etcd 动态发现区块链节点和配置。代理监听 `PUT` 和 `DELETE` 事件，实时添加/移除节点。

### Key 结构

所有 Key 使用配置的 `etcd_prefix` 作为前缀：

| Key 模式 | 说明 |
|----------|------|
| `{prefix}/{chainId}/nodes/{nodeKey}` | 状态/归档节点 |
| `{prefix}/{chainId}/nativeNodes/{nodeKey}` | 原生回退节点 |
| `{prefix}/{chainId}/lastBlockNumber` | 当前链区块高度 |
| `{prefix}/{chainId}/gateway` | 网关配置（权重、方法路由） |
| `{prefix}/{chainId}/mirror/{addrKey}` | 镜像目标 |
| `{prefix}/{chainId}/version` | 链版本信息 |
| `{prefix}/{chainId}/{version}/nodes/{nodeKey}` | 版本化节点 |

### 注册节点

使用 `etcdctl` 注册节点：

```bash
# 为链 1 注册一个状态节点
etcdctl put "/myprefix/1/nodes/node1" '{"url":"http://geth-node1:8545","weight":100}'

# 注册一个原生回退节点
etcdctl put "/myprefix/1/nativeNodes/native1" '{"url":"http://native-node1:8545"}'

# 设置当前区块高度
etcdctl put "/myprefix/1/lastBlockNumber" "20000000"
```

### 节点健康检查

当通过 etcd 发现新节点时：

1. 发送 `eth_blockNumber` RPC 调用验证节点可达性
2. 失败时每 5 秒重试（通过 `node_health_check_timeout` 配置）
3. 最大等待时间：300 秒（通过 `node_health_check_max_wait` 配置）
4. 只有健康检查通过后，节点才会被加入负载均衡池

## 生产环境调优

### 连接池

```yaml
proxy_config:
  connection_pool_size: 2000    # 根据预期并发连接数调整
```

高流量部署时，适当增大此值。监控连接相关错误并相应调整。

### 超时设置

```yaml
proxy_config:
  default_rpc_timeout: 5000       # RPC 调用默认超时（毫秒）
  node_health_check_timeout: 5    # 健康检查重试间隔（秒）
  node_health_check_max_wait: 300 # 节点健康等待最大时间（秒）
```

### 限流

配置按方法的速率限制以保护后端节点：

```yaml
proxy_config:
  processor:
    rate_limiter:
      rpc_methods:
        eth_call: 100          # 每秒请求数
        eth_getLogs: 50
        eth_getBlockByNumber: 200
```

### 日志采样

高流量环境下，启用日志采样以减少日志量：

```yaml
proxy_config:
  observability:
    log:
      sampling:
        enable: true
        initial: 100         # 每秒首先记录 N 条
        thereafter: 100      # 之后每 N 条记录一条
```

### 慢请求阈值

```yaml
proxy_config:
  processor:
    observability_log:
      enable: true
      enable_error_log: true
      slow_threshold:
        default: 500         # 默认慢请求阈值（毫秒）
        rpc_methods:
          eth_call: 1000     # 按方法覆盖
          eth_getLogs: 2000
```

### 系统限制（裸机部署）

高流量生产部署时，增加系统文件描述符限制：

```bash
# /etc/security/limits.conf
nodex    soft    nofile    65535
nodex    hard    nofile    65535
```
