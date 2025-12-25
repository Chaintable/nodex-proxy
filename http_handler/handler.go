package http_handler

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/discovery/etcd"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/lb/selector"
	"github.com/Chaintable/nodex-proxy/lb/selector/random"
	"github.com/Chaintable/nodex-proxy/lb/selector/roundrobin"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// MirrorHandler is the interface that handler needs from LoadBalancer
type MirrorHandler interface {
	GetMirrorMap() jsonrpc.MirrorMap
	GetMirrorLimiter() jsonrpc.MirrorLimiter
}

type Handler struct {
	nodeSelector  selector.Strategy
	etcdClient    *clientv3.Client
	keyPrefix     string
	nodeChannel   chan<- *discovery.TargetNode
	mirrorHandler MirrorHandler
}

func NewHandler(ctx context.Context, nodeSelector selector.Strategy, etcdEndpoints []string, keyPrefix string, nodeChannel chan<- *discovery.TargetNode, mirrorHandler MirrorHandler) (*Handler, error) {
	etcdClient, err := etcd.NewEtcdClient(ctx, etcdEndpoints)
	if err != nil {
		return nil, err
	}
	return &Handler{
		nodeSelector:  nodeSelector,
		etcdClient:    etcdClient,
		keyPrefix:     keyPrefix,
		nodeChannel:   nodeChannel,
		mirrorHandler: mirrorHandler,
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

func (h *Handler) generateGatewayWeight(ctx context.Context, chainId string, nodeKey string, weight int) (*discovery.Gateway, error) {
	gatewayKey := fmt.Sprintf("%s%s/gateway", h.keyPrefix, chainId)
	resp, err := h.etcdClient.Get(ctx, gatewayKey)
	if err != nil {
		return nil, err
	}

	gateway := &discovery.Gateway{}
	if len(resp.Kvs) > 0 {
		err = json.Unmarshal(resp.Kvs[0].Value, &gateway)
		if err != nil {
			return nil, err
		}
	}

	gateway.SetWeight(nodeKey, weight)

	return gateway, nil
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

	gateway, err := h.generateGatewayWeight(ctx, chainId, nodeKey, weight)
	if err != nil {
		return err
	}

	gatewayBytes, err := json.Marshal(gateway)
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
	nativeNodes, _ := h.nodeSelector.GetNativeNodes(chainId)

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

	nativeNodesResp := make([]map[string]interface{}, 0, len(nativeNodes))
	for _, node := range nativeNodes {
		nativeNodesResp = append(nativeNodesResp, map[string]interface{}{
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
		"native_nodes":  nativeNodesResp,
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
		NodeKey  string `json:"node_key"`
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

	if req.NodeKey == "" {
		req.NodeKey = fmt.Sprintf("%s_%d", req.IP, req.Port)
	}

	etcdKey := h.keyPrefix + chainId + "/nodes/" + req.NodeKey
	// todo: check if node already exists
	nodes, err := h.etcdClient.Get(ctx, etcdKey)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to get node from etcd: %v", err)})
		return
	}
	if len(nodes.Kvs) > 0 {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": fmt.Sprintf("node %s already exists", req.NodeKey)})
		return
	}

	nodeData := discovery.TargetNode{
		StateType: req.State,
		Address:   req.IP,
		Port:      req.Port,
		NodeType:  discovery.NodeType(req.NodeType),
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
		gateway, err := h.generateGatewayWeight(ctx, chainId, req.NodeKey, req.Weight)
		if err != nil {
			c.JSON(consts.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to prepare gateway update: %v", err)})
			return
		}

		gatewayBytes, err = json.Marshal(gateway)
		if err != nil {
			c.JSON(consts.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to marshal gateway: %v", err)})
			return
		}
	}

	txn := h.etcdClient.Txn(ctx)
	txn = txn.If(clientv3.Compare(clientv3.Version(etcdKey), "=", 0))
	if req.Weight != types.DefaultWeight {
		gatewayKey := fmt.Sprintf("%s%s/gateway", h.keyPrefix, chainId)
		txn = txn.Then(
			clientv3.OpPut(etcdKey, string(nodeDataBytes)),
			clientv3.OpPut(gatewayKey, string(gatewayBytes)),
		)
	} else {
		txn = txn.Then(clientv3.OpPut(etcdKey, string(nodeDataBytes)))
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
		"node":   req.NodeKey,
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
	nodeKey := c.Param("nodeKey")

	etcdKey := h.keyPrefix + chainId + "/nodes/" + nodeKey

	// Check if node exists
	nodes, err := h.etcdClient.Get(ctx, etcdKey)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to get node from etcd"})
		return
	}
	if len(nodes.Kvs) == 0 {
		c.JSON(consts.StatusNotFound, map[string]string{"error": fmt.Sprintf("node %s not found", nodeKey)})
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

	// Get current Gateway config to clean up method routes
	gateway, modRevision, err := h.getGatewayConfig(ctx, chainId)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to get gateway config: %v", err)})
		return
	}

	gateway.SetWeight(nodeKey, types.DefaultWeight)
	// Clean up method routes - remove the node from all method routes
	gateway.ClearMethodRoute(nodeKey)

	// Prepare gateway update to remove node weight and clean up method routes
	gatewayBytes, err := json.Marshal(gateway)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to marshal gateway config"})
		return
	}

	// Create transaction to delete node and update gateway
	txn := h.etcdClient.Txn(ctx)
	gatewayKey := fmt.Sprintf("%s%s/gateway", h.keyPrefix, chainId)

	// Delete node and update gateway in the same transaction
	txnResp, err := txn.
		If(clientv3.Compare(clientv3.ModRevision(gatewayKey), "=", modRevision)).
		Then(
			clientv3.OpDelete(etcdKey),
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

	c.JSON(consts.StatusOK, map[string]string{"message": fmt.Sprintf("node %s deleted successfully", nodeKey)})
}

func (h *Handler) UpdateNode(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	nodeKey := c.Param("nodeKey")

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

	etcdKey := h.keyPrefix + chainId + "/nodes/" + nodeKey

	// Check if node exists
	nodes, err := h.etcdClient.Get(ctx, etcdKey)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to get node from etcd"})
		return
	}
	if len(nodes.Kvs) == 0 {
		c.JSON(consts.StatusNotFound, map[string]string{"error": fmt.Sprintf("node %s not found", nodeKey)})
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

	_, err = h.etcdClient.Put(ctx, etcdKey, string(nodeDataBytes))
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to update node in etcd"})
		return
	}

	c.JSON(consts.StatusOK, map[string]interface{}{
		"nodeKey":  nodeKey,
		"ip":       nodeData.Address,
		"port":     nodeData.Port,
		"nodeType": nodeData.NodeType,
		"state":    nodeData.StateType,
	})
}

// getGatewayConfig retrieves and parses gateway configuration from etcd
func (h *Handler) getGatewayConfig(ctx context.Context, chainId string) (*discovery.Gateway, int64, error) {
	gatewayKey := fmt.Sprintf("%s%s/gateway", h.keyPrefix, chainId)
	resp, err := h.etcdClient.Get(ctx, gatewayKey)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get gateway config: %v", err)
	}

	var gateway discovery.Gateway
	var modRevision int64
	if len(resp.Kvs) > 0 {
		err = json.Unmarshal(resp.Kvs[0].Value, &gateway)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to parse gateway config: %v", err)
		}
		modRevision = resp.Kvs[0].ModRevision
	}

	return &gateway, modRevision, nil
}

// updateGatewayConfigWithTransaction updates gateway configuration in etcd using transaction to ensure atomicity
func (h *Handler) updateGatewayConfigWithTransaction(ctx context.Context, chainId string, gateway *discovery.Gateway, modRevision int64) error {
	gatewayKey := fmt.Sprintf("%s%s/gateway", h.keyPrefix, chainId)
	gatewayBytes, err := json.Marshal(gateway)
	if err != nil {
		return fmt.Errorf("failed to marshal gateway config: %v", err)
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
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	if !txnResp.Succeeded {
		return fmt.Errorf("concurrent modification detected, please retry")
	}

	return nil
}

// convertMethodRouteToResponse converts a MethodRoute to response format
func (h *Handler) convertMethodRouteToResponse(route discovery.MethodRoute) ([]string, []string) {
	includeKeys := make([]string, 0, len(route.IncludeNodeKeys))
	excludeKeys := make([]string, 0, len(route.ExcludeNodeKeys))

	for key := range route.IncludeNodeKeys {
		includeKeys = append(includeKeys, key)
	}

	for key := range route.ExcludeNodeKeys {
		excludeKeys = append(excludeKeys, key)
	}
	sort.Strings(includeKeys)
	sort.Strings(excludeKeys)

	return includeKeys, excludeKeys
}

// AddMethodRoute handles adding a new method route
func (h *Handler) AddMethodRoute(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")

	var req AddMethodRouteRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "invalid request format"})
		return
	}

	err := h.checkNodeExist(chainId, req.IncludeNodeKeys, req.ExcludeNodeKeys)
	if err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	// Validate required fields
	if req.Method == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "method is required"})
		return
	}

	// Get current Gateway config
	gateway, modRevision, err := h.getGatewayConfig(ctx, chainId)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	if gateway.MethodRoutes == nil {
		gateway.MethodRoutes = make(map[string]discovery.MethodRoute)
	}

	// Check if route already exists for this method and replace it
	route, exists := gateway.MethodRoutes[req.Method]
	if exists {
		for _, nodeKey := range req.IncludeNodeKeys {
			route.IncludeNodeKeys[nodeKey] = true
		}
		for _, nodeKey := range req.ExcludeNodeKeys {
			route.ExcludeNodeKeys[nodeKey] = true
		}
	} else {
		route := discovery.MethodRoute{
			IncludeNodeKeys: make(map[string]bool),
			ExcludeNodeKeys: make(map[string]bool),
		}

		for _, nodeKey := range req.IncludeNodeKeys {
			route.IncludeNodeKeys[nodeKey] = true
		}
		for _, nodeKey := range req.ExcludeNodeKeys {
			route.ExcludeNodeKeys[nodeKey] = true
		}
		gateway.MethodRoutes[req.Method] = route
	}

	// Update ETCD with transaction
	err = h.updateGatewayConfigWithTransaction(ctx, chainId, gateway, modRevision)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	includeKeys, excludeKeys := h.convertMethodRouteToResponse(gateway.MethodRoutes[req.Method])

	c.JSON(consts.StatusOK, AddMethodRouteResponse{
		Method:          req.Method,
		IncludeNodeKeys: includeKeys,
		ExcludeNodeKeys: excludeKeys,
	})
}

func (h *Handler) checkNodeExist(chainId string, includeNodeKeys []string, excludeNodeKeys []string) error {
	// At least one of include_node_ids or exclude_node_ids should be provided
	if len(includeNodeKeys) == 0 && len(excludeNodeKeys) == 0 {
		return fmt.Errorf("at least one of include or exclude is required")
	}

	nodes, ok := h.nodeSelector.GetAllNodes(chainId)
	if !ok {
		return fmt.Errorf("no nodes found for chainID %s", chainId)
	}

	nodeMap := make(map[string]bool)
	for _, node := range nodes {
		nodeMap[node.Key()] = true
	}

	for _, nodeKey := range includeNodeKeys {
		if !nodeMap[nodeKey] {
			return fmt.Errorf("node %s not found", nodeKey)
		}
	}

	for _, nodeKey := range excludeNodeKeys {
		if !nodeMap[nodeKey] {
			return fmt.Errorf("node %s not found", nodeKey)
		}
	}

	return nil
}

// DeleteMethodRoute handles deleting a method route
func (h *Handler) DeleteMethodRoute(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	method := c.Param("method")

	// Get current Gateway config
	gateway, modRevision, err := h.getGatewayConfig(ctx, chainId)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	_, exists := gateway.MethodRoutes[method]
	if !exists {
		c.JSON(consts.StatusNotFound, map[string]string{"error": fmt.Sprintf("method route(%s) not found", method)})
		return
	}

	delete(gateway.MethodRoutes, method)

	// Update ETCD with transaction
	err = h.updateGatewayConfigWithTransaction(ctx, chainId, gateway, modRevision)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	c.JSON(consts.StatusOK, map[string]string{"message": "method route deleted successfully"})
}

// GetMethodRoutes handles getting all method routes for a chain
func (h *Handler) GetMethodRoutes(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")

	// Get current Gateway config
	gateway, _, err := h.getGatewayConfig(ctx, chainId)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	c.JSON(consts.StatusOK, map[string]interface{}{
		"method_routes": gateway.MethodRoutes,
	})
}

// GetMethodRoute handles getting a specific method route
func (h *Handler) GetMethodRoute(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	method := c.Param("method")

	// Get current Gateway config
	gateway, _, err := h.getGatewayConfig(ctx, chainId)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Find specified route by method
	route, exists := gateway.MethodRoutes[method]
	if !exists {
		c.JSON(consts.StatusNotFound, map[string]string{"error": fmt.Sprintf("method route(%s) not found", method)})
		return
	}

	c.JSON(consts.StatusOK, map[string]interface{}{
		"method_route": route,
	})
}

// RemoveMethodRoute handles removing specific nodes from an existing method route
func (h *Handler) RemoveMethodRoute(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")

	var req RemoveMethodRouteRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "invalid request format"})
		return
	}

	err := h.checkNodeExist(chainId, req.IncludeNodeKeys, req.ExcludeNodeKeys)
	if err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	// Validate required fields
	if req.Method == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "method is required"})
		return
	}

	// Get current Gateway config
	gateway, modRevision, err := h.getGatewayConfig(ctx, chainId)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	if gateway.MethodRoutes == nil {
		c.JSON(consts.StatusNotFound, map[string]string{"error": fmt.Sprintf("method route(%s) not found", req.Method)})
		return
	}

	// Check if route exists for this method
	route, exists := gateway.MethodRoutes[req.Method]
	if !exists {
		c.JSON(consts.StatusNotFound, map[string]string{"error": fmt.Sprintf("method route(%s) not found", req.Method)})
		return
	}

	// Remove nodes from include_node_keys
	for _, nodeKey := range req.IncludeNodeKeys {
		delete(route.IncludeNodeKeys, nodeKey)
	}
	if len(route.IncludeNodeKeys) == 0 {
		delete(gateway.MethodRoutes, req.Method)
	}

	// Remove nodes from exclude_node_keys
	for _, nodeKey := range req.ExcludeNodeKeys {
		delete(route.ExcludeNodeKeys, nodeKey)
	}
	if len(route.ExcludeNodeKeys) == 0 {
		delete(gateway.MethodRoutes, req.Method)
	}

	// Update the route in gateway
	if len(route.IncludeNodeKeys) == 0 && len(route.ExcludeNodeKeys) == 0 {
		delete(gateway.MethodRoutes, req.Method)
	} else {
		gateway.MethodRoutes[req.Method] = route
	}

	// Update ETCD with transaction
	err = h.updateGatewayConfigWithTransaction(ctx, chainId, gateway, modRevision)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	includeKeys, excludeKeys := h.convertMethodRouteToResponse(gateway.MethodRoutes[req.Method])

	c.JSON(consts.StatusOK, RemoveMethodRouteResponse{
		Method:          req.Method,
		IncludeNodeKeys: includeKeys,
		ExcludeNodeKeys: excludeKeys,
	})
}

func (h *Handler) AddLocalNode(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")

	type AddNodeRequest struct {
		NodeType int    `json:"node_type"`
		NodeKey  string `json:"node_key"`
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

	if req.IP == "" || req.Port == 0 {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "(ip, port) are required"})
		return
	}

	if req.NodeType != 1 && req.NodeType != 2 {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "invalid node_type value"})
		return
	}

	if req.State < 1 || req.State > 3 {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "state must be between 1 and 3"})
		return
	}

	if req.Weight < 0 || req.Weight > types.DefaultWeight {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "weight must be between 0 and 100"})
		return
	}

	if req.NodeKey == "" {
		req.NodeKey = fmt.Sprintf("%s_%d", req.IP, req.Port)
	}

	localNode := &discovery.TargetNode{
		ChainId:    chainId,
		NodeKey:    req.NodeKey,
		Address:    req.IP,
		Port:       req.Port,
		NodeType:   discovery.NodeType(req.NodeType),
		StateType:  req.State,
		Weight:     req.Weight,
		Source:     "local",
		ChangeType: 0,
	}

	select {
	case h.nodeChannel <- localNode:
		log.Info("AddLocalNode success",
			log.Any("chain_id", chainId),
			log.Any("node_key", req.NodeKey),
			log.Any("ip", req.IP),
			log.Any("port", req.Port),
			log.Any("node_type", req.NodeType),
			log.Any("state", req.State),
			log.Any("weight", req.Weight))
		c.JSON(consts.StatusOK, map[string]interface{}{
			"node":   req.NodeKey,
			"ip":     req.IP,
			"port":   req.Port,
			"state":  req.State,
			"weight": req.Weight,
			"source": "local",
		})
	case <-time.After(time.Second):
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to process node event"})
	}
}

func (h *Handler) DeleteLocalNode(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	nodeKey := c.Param("nodeKey")

	node, err := h.findNode(chainId, nodeKey)
	if err != nil {
		c.JSON(consts.StatusNotFound, map[string]string{"error": "node not found"})
		return
	}

	deleteEvent := &discovery.TargetNode{
		ChainId:    chainId,
		NodeKey:    nodeKey,
		Address:    node.IP(),
		Port:       node.Port(),
		NodeType:   node.NodeType,
		StateType:  node.State(),
		Source:     "local",
		ChangeType: 1,
	}

	select {
	case h.nodeChannel <- deleteEvent:
		log.Info("DeleteLocalNode success",
			log.Any("chain_id", chainId),
			log.Any("node_key", nodeKey))
		c.JSON(consts.StatusOK, map[string]string{"message": fmt.Sprintf("node %s deleted successfully", nodeKey)})
	case <-time.After(time.Second):
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to process delete event"})
	}
}

// Mirror management API methods

// AddMirrorRequest request body for adding mirror target
type AddMirrorRequest struct {
	Address   string `json:"address"`
	Port      int    `json:"port"`
	RateLimit *int   `json:"rateLimit,omitempty"`
}

// MirrorTargetResponse response structure for mirror target
type MirrorTargetResponse struct {
	Address   string `json:"address"`
	Port      int    `json:"port"`
	URL       string `json:"url"`
	RateLimit *int   `json:"rateLimit,omitempty"`
}

// AddLocalMirror adds or updates a mirror target in memory for a specific chain
// POST /:chainId/addLocalMirror
func (h *Handler) AddLocalMirror(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	if chainId == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "chainId is required"})
		return
	}

	var req AddMirrorRequest
	if err := c.Bind(&req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": fmt.Sprintf("invalid request body: %v", err)})
		return
	}

	if req.Address == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "address is required"})
		return
	}

	if req.Port <= 0 || req.Port > 65535 {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "port must be between 1 and 65535"})
		return
	}

	// Assemble addrKey
	addrKey := fmt.Sprintf("%s:%d", req.Address, req.Port)

	// Create mirror target
	target := &discovery.MirrorTarget{
		ChainId:   chainId,
		AddrKey:   addrKey,
		Address:   req.Address,
		Port:      req.Port,
		RateLimit: req.RateLimit,
	}

	// Add to MirrorMap
	h.mirrorHandler.GetMirrorMap().AddMirrorTarget(chainId, addrKey, target)

	// Update rate limiter if configured
	if req.RateLimit != nil && *req.RateLimit > 0 {
		h.mirrorHandler.GetMirrorLimiter().UpdateLimit(chainId, target.URL(), *req.RateLimit)
	}

	c.JSON(consts.StatusCreated, map[string]interface{}{
		"message": "mirror target added successfully",
		"mirror": MirrorTargetResponse{
			Address:   target.Address,
			Port:      target.Port,
			URL:       target.URL(),
			RateLimit: target.RateLimit,
		},
	})
}

// DeleteMirrorRequest request structure for deleting specific mirror
type DeleteMirrorRequest struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

// DeleteLocalMirror deletes a specific mirror target from memory by address and port
// DELETE /:chainId/deleteLocalMirror
func (h *Handler) DeleteLocalMirror(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	if chainId == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "chainId is required"})
		return
	}

	var req DeleteMirrorRequest
	if err := c.Bind(&req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": fmt.Sprintf("invalid request body: %v", err)})
		return
	}

	if req.Address == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "address is required"})
		return
	}

	if req.Port <= 0 || req.Port > 65535 {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "port must be between 1 and 65535"})
		return
	}

	// Assemble addrKey
	addrKey := fmt.Sprintf("%s:%d", req.Address, req.Port)
	mirrorURL := fmt.Sprintf("http://%s:%d", req.Address, req.Port)

	// Delete from MirrorMap
	h.mirrorHandler.GetMirrorMap().DeleteMirrorTarget(chainId, addrKey)

	// Remove from rate limiter
	h.mirrorHandler.GetMirrorLimiter().RemoveLimit(chainId, mirrorURL)

	c.JSON(consts.StatusOK, map[string]string{
		"message": "mirror target deleted successfully",
	})
}

// DeleteAllLocalMirrors deletes all mirror targets from memory for a specific chain
// DELETE /:chainId/deleteAllLocalMirrors
func (h *Handler) DeleteAllLocalMirrors(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	if chainId == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "chainId is required"})
		return
	}

	// Delete all mirrors for the chain
	h.mirrorHandler.GetMirrorMap().DeleteChainMirrors(chainId)

	c.JSON(consts.StatusOK, map[string]string{
		"message": "all mirror targets deleted successfully",
	})
}

// GetMirrors gets all mirror targets for a specific chain
// GET /:chainId/getMirrors
func (h *Handler) GetMirrors(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	if chainId == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "chainId is required"})
		return
	}

	targets := h.mirrorHandler.GetMirrorMap().GetMirrorTargets(chainId)

	mirrors := make([]MirrorTargetResponse, 0, len(targets))
	for _, target := range targets {
		mirrors = append(mirrors, MirrorTargetResponse{
			Address:   target.Address,
			Port:      target.Port,
			URL:       target.URL(),
			RateLimit: target.RateLimit,
		})
	}

	c.JSON(consts.StatusOK, map[string]interface{}{
		"chain_id": chainId,
		"mirrors":  mirrors,
		"count":    len(mirrors),
	})
}

// GetAllMirrors gets all mirror targets for all chains
// GET /getAllMirrors
func (h *Handler) GetAllMirrors(ctx context.Context, c *app.RequestContext) {
	// Get all mirrors directly from MirrorMap
	allMirrors := h.mirrorHandler.GetMirrorMap().GetAllMirrors()

	result := make(map[string][]MirrorTargetResponse)

	for chainId, targets := range allMirrors {
		mirrors := make([]MirrorTargetResponse, 0, len(targets))
		for _, target := range targets {
			mirrors = append(mirrors, MirrorTargetResponse{
				Address:   target.Address,
				Port:      target.Port,
				URL:       target.URL(),
				RateLimit: target.RateLimit,
			})
		}
		result[chainId] = mirrors
	}

	c.JSON(consts.StatusOK, map[string]interface{}{
		"mirrors": result,
	})
}

// AddMirror adds a mirror target to etcd for persistence
// POST /:chainId/addMirror
func (h *Handler) AddMirror(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	if chainId == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "chainId is required"})
		return
	}

	var req AddMirrorRequest
	if err := c.Bind(&req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": fmt.Sprintf("invalid request body: %v", err)})
		return
	}

	if req.Address == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "address is required"})
		return
	}

	if req.Port <= 0 || req.Port > 65535 {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "port must be between 1 and 65535"})
		return
	}

	addrKey := fmt.Sprintf("%s:%d", req.Address, req.Port)
	etcdKey := fmt.Sprintf("%s%s/mirror/%s", h.keyPrefix, chainId, addrKey)

	resp, err := h.etcdClient.Get(ctx, etcdKey)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to get mirror from etcd: %v", err)})
		return
	}
	if len(resp.Kvs) > 0 {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": fmt.Sprintf("mirror %s already exists", addrKey)})
		return
	}

	mirrorData := discovery.MirrorTarget{
		Address:   req.Address,
		Port:      req.Port,
		RateLimit: req.RateLimit,
	}

	mirrorDataBytes, err := json.Marshal(mirrorData)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to marshal mirror data: %v", err)})
		return
	}

	txn := h.etcdClient.Txn(ctx)
	txnResp, err := txn.
		If(clientv3.Compare(clientv3.Version(etcdKey), "=", 0)).
		Then(clientv3.OpPut(etcdKey, string(mirrorDataBytes))).
		Commit()

	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to commit transaction: %v", err)})
		return
	}

	if !txnResp.Succeeded {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "concurrent modification detected, please retry"})
		return
	}

	c.JSON(consts.StatusCreated, map[string]interface{}{
		"message": "mirror target added successfully",
		"mirror": MirrorTargetResponse{
			Address:   req.Address,
			Port:      req.Port,
			URL:       fmt.Sprintf("http://%s:%d", req.Address, req.Port),
			RateLimit: req.RateLimit,
		},
	})
}

// DeleteMirror deletes a specific mirror target from etcd
// DELETE /:chainId/deleteMirror
func (h *Handler) DeleteMirror(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	if chainId == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "chainId is required"})
		return
	}

	var req DeleteMirrorRequest
	if err := c.Bind(&req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": fmt.Sprintf("invalid request body: %v", err)})
		return
	}

	if req.Address == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "address is required"})
		return
	}

	if req.Port <= 0 || req.Port > 65535 {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "port must be between 1 and 65535"})
		return
	}

	addrKey := fmt.Sprintf("%s:%d", req.Address, req.Port)
	etcdKey := fmt.Sprintf("%s%s/mirror/%s", h.keyPrefix, chainId, addrKey)

	resp, err := h.etcdClient.Get(ctx, etcdKey)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": "failed to get mirror from etcd"})
		return
	}
	if len(resp.Kvs) == 0 {
		c.JSON(consts.StatusNotFound, map[string]string{"error": fmt.Sprintf("mirror %s not found", addrKey)})
		return
	}

	_, err = h.etcdClient.Delete(ctx, etcdKey)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to delete mirror from etcd: %v", err)})
		return
	}

	c.JSON(consts.StatusOK, map[string]string{
		"message": "mirror target deleted successfully",
	})
}

// DeleteAllMirrors deletes all mirror targets from etcd for a specific chain
// DELETE /:chainId/deleteAllMirrors
func (h *Handler) DeleteAllMirrors(ctx context.Context, c *app.RequestContext) {
	chainId := c.Param("chainId")
	if chainId == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "chainId is required"})
		return
	}

	etcdPrefix := fmt.Sprintf("%s%s/mirror/", h.keyPrefix, chainId)

	_, err := h.etcdClient.Delete(ctx, etcdPrefix, clientv3.WithPrefix())
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("failed to delete mirrors from etcd: %v", err)})
		return
	}

	c.JSON(consts.StatusOK, map[string]string{
		"message": "all mirror targets deleted successfully",
	})
}
