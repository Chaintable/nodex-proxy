package weight

import (
	"context"
	"strconv"

	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/lb/selector"
	"github.com/Chaintable/nodex-proxy/lb/selector/roundrobin"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// Handler handles weight management operations
type Handler struct {
	nodeSelector selector.Strategy
}

// NewHandler creates a new weight management handler
func NewHandler(nodeSelector selector.Strategy) *Handler {
	return &Handler{
		nodeSelector: nodeSelector,
	}
}

// SetWeight handles setting node weight
func (h *Handler) SetWeight(ctx context.Context, c *app.RequestContext) {
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

// GetWeight handles getting node weight
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

// DeleteWeight handles resetting node weight to default
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

// GetNodes handles getting all nodes for a chain
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
		})
	}

	stateNodesResp := make([]map[string]interface{}, 0, len(stateNodes))
	for _, node := range stateNodes {
		stateNodesResp = append(stateNodesResp, map[string]interface{}{
			"key":    node.Key(),
			"weight": node.Weight(),
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
