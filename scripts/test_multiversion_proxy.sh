#!/usr/bin/env bash
set -euo pipefail

# 配置参数
ETCD_ENDPOINTS=(
  "http://172.21.59.215:2379"
  "http://172.21.59.215:2479"
  "http://172.21.59.215:2579"
)
CHAIN_ID="1"
PROXY_URL="http://127.0.0.1:8780"
RPC_METHOD="eth_blockNumber"
RPC_PARAMS="[]"

REQUIRED_TOOLS=(etcdctl curl)
for tool in "${REQUIRED_TOOLS[@]}"; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "error: 缺少依赖工具 $tool" >&2
    exit 1
  fi
done

JOINED_ENDPOINTS=$(IFS=','; echo "${ETCD_ENDPOINTS[*]}")
PREFIX="${CHAIN_ID}/"

echo "=========================================="
echo "[1] 从 etcd 查询所有 CHAIN_VERSION"
echo "=========================================="
echo "etcd endpoints: $JOINED_ENDPOINTS"
echo "prefix: $PREFIX"
echo ""

# 获取所有 key 并提取唯一的 CHAIN_VERSION
KEYS=$(ETCDCTL_API=3 etcdctl --endpoints "$JOINED_ENDPOINTS" get --prefix "$PREFIX" --keys-only 2>/dev/null || true)

if [[ -z "$KEYS" ]]; then
  echo "error: etcd 未返回任何数据" >&2
  exit 1
fi

# 提取 CHAIN_VERSION (格式: chainId/chainVersion/...)，排除没有版本的 key
VERSIONS=$(echo "$KEYS" | grep -v '^$' | awk -F'/' '{if(NF>=3 && $2!="nodes" && $2!="weights" && $2!="outer_block_notice" && $2!="version") print $2}' | sort -u)

if [[ -z "$VERSIONS" ]]; then
  echo "warning: 未找到任何 CHAIN_VERSION" >&2
  exit 1
fi

# 函数：获取指定前缀下的节点列表
get_nodes() {
  local prefix="$1"
  ETCDCTL_API=3 etcdctl --endpoints "$JOINED_ENDPOINTS" get --prefix "${prefix}nodes/" --keys-only 2>/dev/null | \
    grep -v '^$' | sed "s|${prefix}nodes/||g" | tr '_' ':' | sort
}

# 函数：获取指定前缀下的 version 值
get_version() {
  local prefix="$1"
  ETCDCTL_API=3 etcdctl --endpoints "$JOINED_ENDPOINTS" get "${prefix}version" 2>/dev/null | tail -1
}

echo "发现的 CHAIN_VERSION 列表:"
# 默认版本的节点和 version
DEFAULT_NODES=$(get_nodes "${CHAIN_ID}/")
DEFAULT_VERSION=$(get_version "${CHAIN_ID}/")
echo "  - (默认版本，无 CHAIN_VERSION)"
if [[ -n "$DEFAULT_VERSION" ]]; then
  echo "      version: $DEFAULT_VERSION"
fi
if [[ -n "$DEFAULT_NODES" ]]; then
  echo "$DEFAULT_NODES" | while read -r node; do echo "      节点: $node"; done
fi

# 各版本的节点和 version
echo "$VERSIONS" | while read -r v; do
  echo "  - $v"
  VERSION_VALUE=$(get_version "${CHAIN_ID}/${v}/")
  if [[ -n "$VERSION_VALUE" ]]; then
    echo "      version: $VERSION_VALUE"
  fi
  VERSION_NODES=$(get_nodes "${CHAIN_ID}/${v}/")
  if [[ -n "$VERSION_NODES" ]]; then
    echo "$VERSION_NODES" | while read -r node; do echo "      节点: $node"; done
  fi
done
echo ""

# 逐个版本请求 proxy（包含默认版本）
VERSION_COUNT=$(echo "$VERSIONS" | wc -l | tr -d ' ')
TOTAL_COUNT=$((VERSION_COUNT + 1))  # +1 for default version
CURRENT=0

echo "=========================================="
echo "[2] 逐个请求 proxy (共 $TOTAL_COUNT 个版本)"
echo "=========================================="

# 先测试默认版本（只有 CHAIN_ID）
CURRENT=$((CURRENT + 1))
REQUEST_ID="default-$(date +%s)"
TARGET_URL="${PROXY_URL%/}/${CHAIN_ID}"

REQUEST_BODY=$(cat <<EOF
{"jsonrpc":"2.0","id":"$REQUEST_ID","method":"$RPC_METHOD","params":$RPC_PARAMS}
EOF
)

echo ""
echo "------------------------------------------"
echo "[$CURRENT/$TOTAL_COUNT] 默认版本 (仅 CHAIN_ID: $CHAIN_ID)"
echo "------------------------------------------"
echo "URL: $TARGET_URL"
echo "Request: $REQUEST_BODY"
echo ""

RESPONSE=$(curl -sS -H 'Content-Type: application/json' -X POST -d "$REQUEST_BODY" "$TARGET_URL" 2>&1 || true)

echo "Response:"
if command -v jq >/dev/null 2>&1; then
  echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"
else
  echo "$RESPONSE"
fi

# 再逐个测试带 CHAIN_VERSION 的版本
echo "$VERSIONS" | while read -r CHAIN_VERSION; do
  CURRENT=$((CURRENT + 1))
  REQUEST_ID="${CHAIN_VERSION}-$(date +%s)"
  ROUTE_CHAIN="${CHAIN_ID}-${CHAIN_VERSION}"
  TARGET_URL="${PROXY_URL%/}/$ROUTE_CHAIN"
  
  REQUEST_BODY=$(cat <<EOF
{"jsonrpc":"2.0","id":"$REQUEST_ID","method":"$RPC_METHOD","params":$RPC_PARAMS}
EOF
)

  echo ""
  echo "------------------------------------------"
  echo "[$CURRENT/$TOTAL_COUNT] CHAIN_VERSION: $CHAIN_VERSION"
  echo "------------------------------------------"
  echo "URL: $TARGET_URL"
  echo "Request: $REQUEST_BODY"
  echo ""
  
  RESPONSE=$(curl -sS -H 'Content-Type: application/json' -X POST -d "$REQUEST_BODY" "$TARGET_URL" 2>&1 || true)
  
  echo "Response:"
  if command -v jq >/dev/null 2>&1; then
    echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"
  else
    echo "$RESPONSE"
  fi
done

echo ""
echo "=========================================="
echo "[3] 测试完成"
echo "=========================================="
