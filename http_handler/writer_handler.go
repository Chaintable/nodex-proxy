package http_handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// WriterNodeInfo represents the writer node information stored in etcd
type WriterNodeInfo struct {
	NodeID           string   `json:"node_id"`
	NodeXBucket      string   `json:"node_x_bucket"`
	ChainTableBucket string   `json:"chain_table_bucket"`
	Region           string   `json:"region"`
	Brokers          []string `json:"brokers"`
	Topic            string   `json:"topic"`
}

// WriterNodeResponse represents the response for writer node with runtime status
type WriterNodeResponse struct {
	NodeID           string   `json:"node_id"`
	NodeXBucket      string   `json:"node_x_bucket"`
	ChainTableBucket string   `json:"chain_table_bucket"`
	Region           string   `json:"region"`
	Brokers          []string `json:"brokers"`
	Topic            string   `json:"topic"`
	Status           string   `json:"status"` // "leader" or "backup"
}

// WritersListResponse represents the response for listing writers
type WritersListResponse struct {
	Writers       []WriterNodeResponse `json:"writers"`
	CurrentLeader string               `json:"current_leader"`
}

// SwitchLeaderRequest represents the request to switch leader
type SwitchLeaderRequest struct {
	NewLeader string `json:"new_leader"`
}

// SwitchLeaderResponse represents the response after switching leader
type SwitchLeaderResponse struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	OldLeader string `json:"old_leader"`
	NewLeader string `json:"new_leader"`
}

// LeaderStatusResponse represents the current leader status
type LeaderStatusResponse struct {
	CurrentLeader string `json:"current_leader"`
	LeaderSince   string `json:"leader_since,omitempty"`
}

// GetWriters handles GET /<chain_id>/writers
func (h *Handler) GetWriters(ctx context.Context, c *app.RequestContext) {
	chainID := c.Param("chainId")
	if chainID == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{
			"error": "chainId is required",
		})
		return
	}

	writers, err := h.listActiveWriters(ctx, chainID)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to list writers: %v", err),
		})
		return
	}

	currentLeader, err := h.getCurrentLeader(ctx, chainID)
	if err != nil {
		// Log error but don't fail the request
		currentLeader = ""
	}

	// Add status to each writer
	var writerResponses []WriterNodeResponse
	for _, writer := range writers {
		status := "backup"
		if writer.NodeID == currentLeader {
			status = "leader"
		}

		writerResponses = append(writerResponses, WriterNodeResponse{
			NodeID:           writer.NodeID,
			NodeXBucket:      writer.NodeXBucket,
			ChainTableBucket: writer.ChainTableBucket,
			Region:           writer.Region,
			Brokers:          writer.Brokers,
			Topic:            writer.Topic,
			Status:           status,
		})
	}

	response := WritersListResponse{
		Writers:       writerResponses,
		CurrentLeader: currentLeader,
	}

	c.JSON(consts.StatusOK, response)
}

// SwitchLeader handles POST /<chain_id>/writers/switchLeader
func (h *Handler) SwitchLeader(ctx context.Context, c *app.RequestContext) {
	chainID := c.Param("chainId")
	if chainID == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{
			"error": "chainId is required",
		})
		return
	}

	var req SwitchLeaderRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("Invalid request body: %v", err),
		})
		return
	}

	if req.NewLeader == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{
			"error": "new_leader is required",
		})
		return
	}

	// Get current leader before switching
	oldLeader, err := h.getCurrentLeader(ctx, chainID)
	if err != nil {
		oldLeader = ""
	}

	// Perform leader switch
	err = h.switchLeader(ctx, chainID, req.NewLeader)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, SwitchLeaderResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to switch leader: %v", err),
		})
		return
	}

	response := SwitchLeaderResponse{
		Success:   true,
		Message:   "Leader switched successfully",
		OldLeader: oldLeader,
		NewLeader: req.NewLeader,
	}

	c.JSON(consts.StatusOK, response)
}

// GetLeaderStatus handles GET /<chain_id>/writers/leader
func (h *Handler) GetLeaderStatus(ctx context.Context, c *app.RequestContext) {
	chainID := c.Param("chainId")
	if chainID == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{
			"error": "chainId is required",
		})
		return
	}

	currentLeader, err := h.getCurrentLeader(ctx, chainID)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to get leader status: %v", err),
		})
		return
	}

	response := LeaderStatusResponse{
		CurrentLeader: currentLeader,
		// TODO: Add LeaderSince timestamp if needed
	}

	c.JSON(consts.StatusOK, response)
}

// Helper functions

func (h *Handler) listActiveWriters(ctx context.Context, chainID string) ([]*WriterNodeInfo, error) {
	keyPrefix := fmt.Sprintf("%s/writers/", chainID)
	resp, err := h.etcdClient.Get(ctx, keyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to list active writers: %w", err)
	}

	var writers []*WriterNodeInfo

	for _, kv := range resp.Kvs {
		key := string(kv.Key)

		// Extract node ID from key (format: {chain_id}/writers/{node_id})
		if len(key) <= len(keyPrefix) {
			continue
		}
		nodeID := key[len(keyPrefix):]

		// Skip the leader key {chain_id}/writers/leader
		if nodeID == "leader" {
			continue
		}

		var nodeInfo WriterNodeInfo
		if err := json.Unmarshal(kv.Value, &nodeInfo); err != nil {
			// Log error but continue with other nodes
			continue
		}
		nodeInfo.NodeID = nodeID

		writers = append(writers, &nodeInfo)
	}

	return writers, nil
}

func (h *Handler) getCurrentLeader(ctx context.Context, chainID string) (string, error) {
	leaderKey := fmt.Sprintf("%s/writers/leader", chainID)

	ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := h.etcdClient.Get(ctxWithTimeout, leaderKey)
	if err != nil {
		return "", fmt.Errorf("failed to get current leader: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("no leader found for chain %s", chainID)
	}

	return string(resp.Kvs[0].Value), nil
}

func (h *Handler) switchLeader(ctx context.Context, chainID, newLeaderID string) error {
	// 1. Check if the new leader node exists and is online
	writerKey := fmt.Sprintf("%s/writers/%s", chainID, newLeaderID)
	resp, err := h.etcdClient.Get(ctx, writerKey)
	if err != nil {
		return fmt.Errorf("failed to check target node: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return fmt.Errorf("target node %s not found or offline", newLeaderID)
	}

	// 2. Get current leader and version info for atomic update
	leaderKey := fmt.Sprintf("%s/writers/leader", chainID)
	leaderResp, err := h.etcdClient.Get(ctx, leaderKey)
	if err != nil {
		return fmt.Errorf("failed to get current leader: %w", err)
	}

	var modRevision int64
	if len(leaderResp.Kvs) > 0 {
		modRevision = leaderResp.Kvs[0].ModRevision
	}

	// 3. Use transaction for atomic leader update with version control
	txn := h.etcdClient.Txn(ctx)
	txnResp, err := txn.
		If(clientv3.Compare(clientv3.ModRevision(leaderKey), "=", modRevision)).
		Then(clientv3.OpPut(leaderKey, newLeaderID)).
		Else(clientv3.OpGet(leaderKey)).
		Commit()

	if err != nil {
		return fmt.Errorf("failed to switch leader: %w", err)
	}

	if !txnResp.Succeeded {
		return fmt.Errorf("concurrent modification detected, please retry")
	}

	return nil
}
