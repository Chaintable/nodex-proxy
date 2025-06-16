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
	"github.com/Chaintable/nodex-proxy/lb/selector/random"
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

	if weight < 0 || weight > types.DefaultWeight {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "weight must be between 0 and 100"})
		return
	}

	_, err = h.findNode(chainId, nodeKey)
	if err != nil {
		c.JSON(consts.StatusNotFound, map[string]string{"error": "node not found"})
		return
	}

	err = h.setWeight(ctx, chainId, nodeKey, weight)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to set weight: %v", err)})
		return
	}

	c.JSON(consts.StatusOK, map[string]interface{}{
		"node":    nodeKey,
		"weight":  weight,
		"message": "weight updated successfully",
	})
}

func (h *Handler) generateGatewayWeight(ctx context.Context, chainId string, nodeKey string, weight int) ([]byte, error) {
	gatewayKey := fmt.Sprintf("%s%s/gateway", h.keyPrefix, chainId)
	resp, err := h.etcdClient.Get(ctx, gatewayKey)
	if err != nil {
		return nil, err
	}

	gateway := discovery.Gateway{}
	if len(resp.Kvs) > 0 {
		err = json.Unmarshal(resp.Kvs[0].Value, &gateway)
		if err != nil {
			return nil, err
		}
	}

	nodeIndex := -1
	for i, nodeStatus := range gateway.Status {
		if nodeStatus.NodeKey == nodeKey {
			nodeIndex = i
			break
		}
	}

	// only add to gateway if weight is not default(100)
	if weight == types.DefaultWeight {
		if nodeIndex != -1 {
			// if node exists, remove it from gateway
			gateway.Status = append(gateway.Status[:nodeIndex], gateway.Status[nodeIndex+1:]...)
		}
		// if node doesn't exist, do nothing
	} else {
		if nodeIndex != -1 {
			// if node exists, update its weight
			gateway.Status[nodeIndex].Weight = weight
		} else {
			// if node doesn't exist, add it to gateway
			gateway.Status = append(gateway.Status, discovery.GatewayStatus{
				NodeKey: nodeKey,
				Weight:  weight,
			})
		}
	}

	return json.Marshal(gateway)
}

func (h *Handler) setWeight(ctx context.Context, chainId string, nodeKey string, weight int) error {
	gatewayKey := fmt.Sprintf("%s%s/gateway", h.keyPrefix, chainId)

	// Get the current value and revision
	resp, err := h.etcdClient.Get(ctx, gatewayKey)
	if err != nil {
		return err
	}

	var modRevision int64
	if len(resp.Kvs) > 0 {
		modRevision = resp.Kvs[0].ModRevision
	}

	gatewayBytes, err := h.generateGatewayWeight(ctx, chainId, nodeKey, weight)
	if err != nil {
		return err
	}

	// Create a transaction
	txn := h.etcdClient.Txn(ctx)

	// Compare the value hasn't changed since we read it
	txnResp, err := txn.
		If(clientv3.Compare(clientv3.ModRevision(gatewayKey), "=", modRevision)).
		Then(clientv3.OpPut(gatewayKey, string(gatewayBytes))).
		Else(clientv3.OpGet(gatewayKey)).
		Commit()

	if err != nil {
		return err
	}

	if !txnResp.Succeeded {
		return fmt.Errorf("concurrent modification detected, please retry")
	}

	return nil
}

func (h *Handler) GetWeight(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	nodeKey := c.Query("node")

	if chainId == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "chainId parameter is required"})
		return
	}

	if nodeKey == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "node parameter is required"})
		return
	}

	// return weights for all the nodes in the chain
	if nodeKey == "all" {
		weights := h.getChainWeights(chainId)
		c.JSON(consts.StatusOK, map[string]interface{}{
			"weights": weights,
		})
		return
	}

	// return weight for a specific node
	node, err := h.findNode(chainId, nodeKey)
	if err != nil {
		c.JSON(consts.StatusNotFound, map[string]string{"error": "node not found"})
		return
	}

	weight := h.getNodeWeight(chainId, node)

	c.JSON(consts.StatusOK, map[string]interface{}{
		"weights": map[string]int{
			nodeKey: weight,
		},
	})
}

func (h *Handler) findNode(chainId string, nodeKey string) (*lbnode.Node, error) {
	allNodes, _ := h.nodeSelector.GetAllNodes(chainId)

	for _, node := range allNodes {
		if node.Key() == nodeKey {
			return node, nil
		}
	}
	return nil, fmt.Errorf("node not found")
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

	_, err := h.findNode(chainId, nodeKey)
	if err != nil {
		c.JSON(consts.StatusNotFound, map[string]string{"error": "node not found"})
		return
	}

	err = h.setWeight(ctx, chainId, nodeKey, types.DefaultWeight)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to reset weight to default: %v", err)})
		return
	}

	c.JSON(consts.StatusOK, map[string]interface{}{
		"node":    nodeKey,
		"weight":  types.DefaultWeight,
		"message": "reset to default weight 100",
	})
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
			"source": node.Source(),
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
			"source": node.Source(),
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

	if req.Weight < 0 || req.Weight > types.DefaultWeight {
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
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to get node from etcd: %v", err)})
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
		Source:    "manual",
	}

	nodeDataBytes, err := json.Marshal(nodeData)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to marshal node data: %v", err)})
		return
	}

	// Prepare gateway update if weight is not default
	var gatewayBytes []byte
	if req.Weight != types.DefaultWeight {
		gatewayBytes, err = h.generateGatewayWeight(ctx, chainId, req.NodeId, req.Weight)
		if err != nil {
			c.JSON(consts.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to prepare gateway update: %v", err)})
			return
		}
	}

	txn := h.etcdClient.Txn(ctx)
	txn = txn.If(clientv3.Compare(clientv3.Version(nodeKey), "=", 0))
	if req.Weight != types.DefaultWeight {
		gatewayKey := fmt.Sprintf("%s%s/gateway", h.keyPrefix, chainId)
		txn = txn.Then(
			clientv3.OpPut(nodeKey, string(nodeDataBytes)),
			clientv3.OpPut(gatewayKey, string(gatewayBytes)),
		)
	} else {
		txn = txn.Then(clientv3.OpPut(nodeKey, string(nodeDataBytes)))
	}

	// Commit transaction
	txnResp, err := txn.Commit()
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to commit transaction: %v", err)})
		return
	}

	if !txnResp.Succeeded {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "concurrent modification detected, please retry"})
		return
	}

	// Return success response
	c.JSON(consts.StatusOK, map[string]interface{}{
		"node":   req.NodeId,
		"ip":     req.IP,
		"port":   req.Port,
		"state":  req.State,
		"weight": req.Weight,
		"source": "manual",
	})
}

func (h *Handler) getNodeWeight(chainId string, node *lbnode.Node) int {
	switch h.nodeSelector.(type) {
	case *random.Random:
		weight, exists := h.nodeSelector.(*random.Random).GatewayStrategy.GetWeightForNode(chainId, node.Key())
		if !exists {
			return types.DefaultWeight
		}
		return weight
	default:
		return node.Weight()
	}
}

func (h *Handler) getChainWeights(chainId string) map[string]int {
	switch h.nodeSelector.(type) {
	case *random.Random:
		weights, _ := h.nodeSelector.(*random.Random).GatewayStrategy.GetWeightForChain(chainId)
		return weights
	default:
		weights := make(map[string]int)
		return weights
	}
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

	// check source is manual
	var nodeData discovery.TargetNode
	if err := json.Unmarshal(nodes.Kvs[0].Value, &nodeData); err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to parse existing node data"})
		return
	}
	if nodeData.Source != "manual" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "only manual nodes can be deleted"})
		return
	}

	// Prepare gateway update to remove node weight
	gatewayBytes, err := h.generateGatewayWeight(ctx, chainId, nodeId, types.DefaultWeight)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to prepare gateway update"})
		return
	}

	// Create transaction to delete node and update gateway
	txn := h.etcdClient.Txn(ctx)
	gatewayKey := fmt.Sprintf("%s%s/gateway", h.keyPrefix, chainId)

	// Delete node and update gateway in the same transaction
	txnResp, err := txn.
		Then(
			clientv3.OpDelete(nodeKey),
			clientv3.OpPut(gatewayKey, string(gatewayBytes)),
		).
		Commit()

	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to commit transaction: %v", err)})
		return
	}

	if !txnResp.Succeeded {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "concurrent modification detected, please retry"})
		return
	}

	c.JSON(consts.StatusOK, map[string]string{"message": fmt.Sprintf("node %s deleted successfully", nodeId)})
}

func (h *Handler) UpdateNode(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	nodeId := c.Param("nodeId")

	// 定义请求结构体
	type UpdateNodeRequest struct {
		IP   string `json:"ip"`
		Port int    `json:"port"`
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

	if nodeData.Source != "manual" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "only manual nodes can be updated"})
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
		"nodeId":   nodeId,
		"ip":       nodeData.Address,
		"port":     nodeData.Port,
		"nodeType": nodeData.NodeType,
		"state":    nodeData.StateType,
	})
}
