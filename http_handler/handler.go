package http_handler

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/discovery/etcd"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/lb/selector"
	"github.com/Chaintable/nodex-proxy/lb/selector/roundrobin"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Handler struct {
	nodeSelector selector.Strategy
	etcdClient   *clientv3.Client
	keyPrefix    string
}

func NewHandler(ctx context.Context, nodeSelector selector.Strategy, etcdEndpoints []string, keyPrefix string) (*Handler, error) {
	etcdClient, err := etcd.NewEtcdClient(ctx, etcdEndpoints)
	if err != nil {
		return nil, err
	}
	return &Handler{
		nodeSelector: nodeSelector,
		etcdClient:   etcdClient,
		keyPrefix:    keyPrefix,
	}, nil
}

func (h *Handler) GetAllChainsIDs(ctx context.Context, c *app.RequestContext) {
	chains := h.nodeSelector.GetAllChainsIDs()
	c.JSON(consts.StatusOK, map[string]interface{}{
		"chain_ids": chains,
	})
}

func (h *Handler) SetWeight(ctx context.Context, c *app.RequestContext) {
	// todo: use post body instead of query params
	chainId := c.Param("chainId")
	nodeKey := c.Query("node")
	weightStr := c.Query("weight")

	// todo: round-robin doesn't support set weight now
	if _, ok := h.nodeSelector.(*roundrobin.RoundRobin); ok {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "round-robin doesn't support set weight"})
		return
	}

	if nodeKey == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "node parameter is required"})
		return
	}

	weight, err := strconv.Atoi(weightStr)
	if err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "invalid weight value"})
		return
	}

	if weight < 0 || weight > types.DefaultLoadBalancerWeight {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "weight must be between 0 and 100"})
		return
	}

	// Get nodes for the chain
	archiveNodes, _ := h.nodeSelector.GetArchiveNodes(chainId)
	stateNodes, _ := h.nodeSelector.GetStateNodes(chainId)

	// Search in both archive and state nodes
	if h.updateNodeWeight(archiveNodes, nodeKey, weight) || h.updateNodeWeight(stateNodes, nodeKey, weight) {
		c.JSON(consts.StatusOK, map[string]interface{}{
			"node":    nodeKey,
			"weight":  weight,
			"message": "weight updated successfully",
		})
		return
	}

	c.JSON(consts.StatusNotFound, map[string]string{"error": "node not found"})
}

func (h *Handler) GetWeight(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	nodeKey := c.Query("node")

	if nodeKey == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "node parameter is required"})
		return
	}

	// Get nodes for the chain
	archiveNodes, _ := h.nodeSelector.GetArchiveNodes(chainId)
	stateNodes, _ := h.nodeSelector.GetStateNodes(chainId)

	// Search in both archive and state nodes
	if weight, found := h.getNodeWeight(archiveNodes, nodeKey); found {
		c.JSON(consts.StatusOK, map[string]interface{}{
			"node":   nodeKey,
			"weight": weight,
		})
		return
	}

	if weight, found := h.getNodeWeight(stateNodes, nodeKey); found {
		c.JSON(consts.StatusOK, map[string]interface{}{
			"node":   nodeKey,
			"weight": weight,
		})
		return
	}

	c.JSON(consts.StatusNotFound, map[string]string{"error": "node not found"})
}

func (h *Handler) DeleteWeight(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	nodeKey := c.Query("node")

	if nodeKey == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "node parameter is required"})
		return
	}

	// todo: round-robin doesn't support set weight now
	if _, ok := h.nodeSelector.(*roundrobin.RoundRobin); ok {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "round-robin doesn't support set weight"})
		return
	}

	// Get nodes for the chain
	archiveNodes, _ := h.nodeSelector.GetArchiveNodes(chainId)
	stateNodes, _ := h.nodeSelector.GetStateNodes(chainId)

	// Search in both archive and state nodes
	if h.updateNodeWeight(archiveNodes, nodeKey, types.DefaultLoadBalancerWeight) ||
		h.updateNodeWeight(stateNodes, nodeKey, types.DefaultLoadBalancerWeight) {
		c.JSON(consts.StatusOK, map[string]interface{}{
			"node":    nodeKey,
			"weight":  types.DefaultLoadBalancerWeight,
			"message": "reset to default weight 100",
		})
		return
	}

	c.JSON(consts.StatusNotFound, map[string]string{"error": "node not found"})
}

func (h *Handler) GetAllNodes(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")

	// Get nodes for the chain
	archiveNodes, _ := h.nodeSelector.GetArchiveNodes(chainId)
	stateNodes, _ := h.nodeSelector.GetStateNodes(chainId)

	// Convert nodes to response format
	archiveNodesResp := make([]map[string]interface{}, 0, len(archiveNodes))
	for _, node := range archiveNodes {
		archiveNodesResp = append(archiveNodesResp, map[string]interface{}{
			"key":    node.Key(),
			"weight": node.Weight(),
			"ip":     node.IP(),
			"port":   node.Port(),
			"state":  node.State(),
		})
	}

	stateNodesResp := make([]map[string]interface{}, 0, len(stateNodes))
	for _, node := range stateNodes {
		stateNodesResp = append(stateNodesResp, map[string]interface{}{
			"key":    node.Key(),
			"weight": node.Weight(),
			"ip":     node.IP(),
			"port":   node.Port(),
			"state":  node.State(),
		})
	}

	c.JSON(consts.StatusOK, map[string]interface{}{
		"archive_nodes": archiveNodesResp,
		"state_nodes":   stateNodesResp,
	})
}

// ChooseOneNode handles getting a selected node for debugging purposes
func (h *Handler) ChooseOneNode(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	requestKey := c.Query("request_key")

	// Create a request context for node selection
	reqCtx := &types.RequestContext{
		ChainId: chainId,
		BlockContext: &types.BlockContext{
			Type: c.Query("block_context"),
		},
	}

	// Get the selected node
	selectedNode, err := h.nodeSelector.GetNode(reqCtx, requestKey)
	if err != nil {
		c.JSON(consts.StatusOK, map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	response := map[string]interface{}{
		"selected_node": map[string]interface{}{
			"key":    selectedNode.Key(),
			"weight": selectedNode.Weight(),
		},
	}

	c.JSON(consts.StatusOK, response)
}

// AddNode handles adding a new node
func (h *Handler) AddNode(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")

	// 定义请求结构体
	type AddNodeRequest struct {
		NodeType int    `json:"node_type"`
		NodeId   string `json:"node_id"`
		IP       string `json:"ip"`
		Port     int    `json:"port"`
		State    int    `json:"state"`
		Weight   int    `json:"weight"`
	}

	var req AddNodeRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "invalid request format"})
		return
	}

	// Validate required fields
	if req.IP == "" || req.Port == 0 {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "(ip, port) are required"})
		return
	}

	// Validate node type
	if req.NodeType != 1 && req.NodeType != 2 {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "invalid node_type value"})
		return
	}

	// Validate state value
	if req.State < 1 || req.State > 3 {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "state must be between 1 and 3"})
		return
	}

	if req.Weight < 0 || req.Weight > types.DefaultLoadBalancerWeight {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "weight must be between 0 and 100"})
		return
	}

	if req.NodeId == "" {
		req.NodeId = fmt.Sprintf("%s_%d", req.IP, req.Port)
	}

	nodeKey := h.keyPrefix + chainId + "/nodes/" + req.NodeId
	// todo: check if node already exists
	nodes, err := h.etcdClient.Get(ctx, nodeKey)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to get node from etcd"})
		return
	}
	if len(nodes.Kvs) > 0 {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": fmt.Sprintf("node %s already exists", req.NodeId)})
		return
	}

	nodeData := discovery.TargetNode{
		StateType: req.State,
		Address:   req.IP,
		Port:      req.Port,
		NodeType:  req.NodeType,
		Weight:    req.Weight,
	}

	nodeDataBytes, err := json.Marshal(nodeData)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to marshal node data"})
		return
	}

	// todo: txn & add weight
	_, err = h.etcdClient.Put(ctx, nodeKey, string(nodeDataBytes))
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to store node in etcd"})
		return
	}

	// Return success response
	c.JSON(consts.StatusOK, map[string]interface{}{
		"node":   req.NodeId,
		"ip":     req.IP,
		"port":   req.Port,
		"state":  req.State,
		"weight": req.Weight,
	})
}

// Helper functions
func (h *Handler) updateNodeWeight(nodes []*lbnode.Node, nodeKey string, weight int) bool {
	for _, node := range nodes {
		if node.Key() == nodeKey {
			node.SetWeight(weight)
			return true
		}
	}
	return false
}

func (h *Handler) getNodeWeight(nodes []*lbnode.Node, nodeKey string) (int, bool) {
	for _, node := range nodes {
		if node.Key() == nodeKey {
			return node.Weight(), true
		}
	}
	return 0, false
}

func (h *Handler) DeleteNode(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	nodeId := c.Param("nodeId")

	nodeKey := h.keyPrefix + chainId + "/nodes/" + nodeId

	// Check if node exists
	nodes, err := h.etcdClient.Get(ctx, nodeKey)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to get node from etcd"})
		return
	}
	if len(nodes.Kvs) == 0 {
		c.JSON(consts.StatusNotFound, map[string]string{"error": fmt.Sprintf("node %s not found", nodeId)})
		return
	}

	// Delete node from etcd
	_, err = h.etcdClient.Delete(ctx, nodeKey)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to delete node from etcd"})
		return
	}

	c.JSON(consts.StatusOK, map[string]string{"message": fmt.Sprintf("node %s deleted successfully", nodeId)})
}

func (h *Handler) UpdateNode(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	nodeId := c.Param("nodeId")

	// 定义请求结构体
	type UpdateNodeRequest struct {
		IP       string `json:"ip"`
		Port     int    `json:"port"`
	}

	var req UpdateNodeRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "invalid request format"})
		return
	}

	if req.IP == "" && req.Port == 0 {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "at least one field(ip, port) is required"})
		return
	}

	nodeKey := h.keyPrefix + chainId + "/nodes/" + nodeId

	// Check if node exists
	nodes, err := h.etcdClient.Get(ctx, nodeKey)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to get node from etcd"})
		return
	}
	if len(nodes.Kvs) == 0 {
		c.JSON(consts.StatusNotFound, map[string]string{"error": fmt.Sprintf("node %s not found", nodeId)})
		return
	}

	// Parse existing node data
	var nodeData discovery.TargetNode
	if err := json.Unmarshal(nodes.Kvs[0].Value, &nodeData); err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to parse existing node data"})
		return
	}

	// Update node data
	if req.IP != "" {
		nodeData.Address = req.IP
	}
	if req.Port != 0 {
		nodeData.Port = req.Port
	}

	// Marshal and store updated node data
	nodeDataBytes, err := json.Marshal(nodeData)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to marshal node data"})
		return
	}

	_, err = h.etcdClient.Put(ctx, nodeKey, string(nodeDataBytes))
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to update node in etcd"})
		return
	}

	c.JSON(consts.StatusOK, map[string]interface{}{
		"nodeId":    nodeId,
		"ip":        nodeData.Address,
		"port":      nodeData.Port,
		"nodeType": nodeData.NodeType,
		"state":     nodeData.StateType,
	})
}
