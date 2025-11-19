package lb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc/metrics"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"go.opentelemetry.io/otel/attribute"
)

// NodeHealthChecker is responsible for checking node health before adding to the pool
type NodeHealthChecker struct {
	healthCheckTimeout time.Duration
	maxWaitTime        time.Duration
	httpClient         *http.Client
}

// NewNodeHealthChecker creates a new health checker
func NewNodeHealthChecker(healthCheckTimeout, maxWaitTime time.Duration) *NodeHealthChecker {
	return &NodeHealthChecker{
		healthCheckTimeout: healthCheckTimeout,
		maxWaitTime:        maxWaitTime,
		httpClient: &http.Client{
			Timeout: healthCheckTimeout,
		},
	}
}

// CheckNodeHealth performs a health check on a node by querying block height
func (hc *NodeHealthChecker) CheckNodeHealth(ctx context.Context, tempNode *discovery.TargetNode) (*lbnode.Node, error) {
	startTime := time.Now()

	targetNode, err := lbnode.New(
		tempNode.NodeKey,
		tempNode.Address,
		tempNode.Port,
		tempNode.Weight,
		tempNode.NodeType,
		lbnode.WithSource(tempNode.Source),
	)
	if err != nil {
		log.Error("failed to create node for health check",
			err,
			log.Any("node_key", tempNode.NodeKey),
			log.Any("address", tempNode.Address))
		hc.recordHealthCheckMetric(tempNode.ChainId, tempNode.NodeKey, "create_failed", time.Since(startTime))
		return nil, err
	}

	// Set initial state
	targetNode.SetState(tempNode.StateType)

	// Perform health check with retry logic
	startCheckTime := time.Now()
	retryInterval := 5 * time.Second
	attemptCount := 0

	for {
		attemptCount++
		success := hc.processHealthCheck(targetNode)

		if success {
			duration := time.Since(startTime)
			log.Info("node health check passed",
				log.Any("node_key", tempNode.NodeKey),
				log.Any("address", targetNode.Addr()),
				log.Any("chain_id", tempNode.ChainId),
				log.Any("duration_sec", duration.Seconds()),
				log.Any("attempts", attemptCount))
			hc.recordHealthCheckMetric(tempNode.ChainId, tempNode.NodeKey, "success", duration)
			return targetNode, nil
		}

		// Check if we've exceeded maxWaitTime
		elapsed := time.Since(startCheckTime)
		if elapsed >= hc.maxWaitTime {
			duration := time.Since(startTime)
			log.Error("node health check failed after max wait time",
				fmt.Errorf("health check failed after %d attempts", attemptCount),
				log.Any("node_key", tempNode.NodeKey),
				log.Any("address", targetNode.Addr()),
				log.Any("chain_id", tempNode.ChainId),
				log.Any("duration_sec", duration.Seconds()),
				log.Any("attempts", attemptCount),
				log.Any("max_wait_time_sec", hc.maxWaitTime.Seconds()))
			hc.recordHealthCheckMetric(tempNode.ChainId, tempNode.NodeKey, "timeout", duration)
			return nil, fmt.Errorf("health check failed after %d attempts and %v", attemptCount, elapsed)
		}

		// Log retry attempt
		log.Warn("health check failed, retrying",
			log.Any("node_key", tempNode.NodeKey),
			log.Any("address", targetNode.Addr()),
			log.Any("chain_id", tempNode.ChainId),
			log.Any("attempt", attemptCount),
			log.Any("elapsed_sec", elapsed.Seconds()),
			log.Any("max_wait_time_sec", hc.maxWaitTime.Seconds()))

		metrics.IncrNodeHealthCheckTotal(tempNode.ChainId, tempNode.NodeKey, "retry")

		// Wait before retry
		time.Sleep(retryInterval)
	}
}

// processHealthCheck performs the actual health check by querying block height
func (hc *NodeHealthChecker) processHealthCheck(node *lbnode.Node) bool {
	// Construct JSON-RPC request for getting block height
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "getLatestBlock",
		"params":  []interface{}{},
		"id":      1,
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		log.Error("failed to marshal health check request", err)
		return false
	}

	// Make HTTP request
	url := fmt.Sprintf("http://%s", node.Addr())
	req, err := http.NewRequest("POST", url, bytes.NewReader(reqBytes))
	if err != nil {
		log.Error("failed to create health check request",
			err,
			log.Any("url", url))
		return false
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := hc.httpClient.Do(req)
	if err != nil {
		log.Error("health check request failed",
			err,
			log.Any("url", url),
			log.Any("node_key", node.Key()))
		return false
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		log.Warn("health check returned non-200 status",
			log.Any("status_code", resp.StatusCode),
			log.Any("url", url),
			log.Any("node_key", node.Key()))
		return false
	}

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("failed to read health check response",
			err,
			log.Any("url", url))
		return false
	}

	// Parse response
	var rpcResp struct {
		Jsonrpc string          `json:"jsonrpc"`
		Result  json.RawMessage `json:"result,omitempty"`
		Error   *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error,omitempty"`
		ID interface{} `json:"id"`
	}

	if err := json.Unmarshal(body, &rpcResp); err != nil {
		log.Error("failed to unmarshal health check response",
			err,
			log.Any("url", url),
			log.Any("body", string(body)))
		return false
	}

	// Check for RPC error
	if rpcResp.Error != nil {
		log.Warn("health check RPC returned error",
			log.Any("error_code", rpcResp.Error.Code),
			log.Any("error_message", rpcResp.Error.Message),
			log.Any("url", url),
			log.Any("node_key", node.Key()))
		return false
	}

	// Check if result exists
	if len(rpcResp.Result) == 0 {
		log.Warn("health check returned empty result",
			log.Any("url", url),
			log.Any("node_key", node.Key()))
		return false
	}

	log.Debug("health check succeeded",
		log.Any("url", url),
		log.Any("node_key", node.Key()),
		log.Any("result", string(rpcResp.Result)))

	return true
}

// recordHealthCheckMetric records health check metrics
func (hc *NodeHealthChecker) recordHealthCheckMetric(chainId, nodeKey, status string, duration time.Duration) {
	// Use metrics package to record metrics
	metrics.IncrNodeHealthCheckTotal(chainId, nodeKey, status)
	metrics.ObserveNodeHealthCheckDuration(chainId, nodeKey, status, duration)

	// Use observability to record metric
	attributes := []attribute.KeyValue{
		attribute.String("chain_id", chainId),
		attribute.String("node_key", nodeKey),
		attribute.String("status", status),
		attribute.Float64("duration_sec", duration.Seconds()),
	}

	log.Observe("node_health_check", context.Background(), attributes...)
}
