package lb

import (
	"context"
	"testing"
	"time"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lb/jsonrpc"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/stretchr/testify/require"
)

func TestNodeDowngradeSkipsHealthCheck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := types.DefaultConfig()
	heightMap := jsonrpc.NewHeightMap()
	nodeCh := make(chan *discovery.TargetNode)
	lbi := NewLoadBalancer(ctx, cfg,
		&jsonrpc.GeneralRPCMethodHertzHandler{Config: &cfg, HeightMap: heightMap},
		jsonrpc.NewMethodLimiter(nil), heightMap, jsonrpc.NewMirrorLimiter(),
		nodeCh, make(chan *discovery.ChainHeight), make(chan *discovery.Gateway),
		make(chan *discovery.MirrorTarget), make(chan *discovery.ChainVersion))
	go lbi.BackgroundRefreshNode()

	// Port 1 on loopback refuses connections, so a health check never passes.
	pooled, err := lbnode.New("pooled", "127.0.0.1", 1, 100, discovery.NodeTypeState, lbnode.WithSource(""))
	require.NoError(t, err)
	pooled.SetState(lbnode.StateAvailable)
	require.NoError(t, lbi.NodeSelector.UpsertNode(ctx, "1", discovery.NodeTypeState, pooled))

	nodeCh <- &discovery.TargetNode{ChainId: "1", NodeKey: "pooled", Address: "127.0.0.1", Port: 1,
		NodeType: discovery.NodeTypeState, StateType: 3, Weight: 100}
	require.Eventually(t, func() bool { return !pooled.Available() }, time.Second, 5*time.Millisecond,
		"an offline update for a pooled node must apply without waiting for a health check")

	// A node that is not in the pool still has to pass the health check.
	nodeCh <- &discovery.TargetNode{ChainId: "1", NodeKey: "fresh", Address: "127.0.0.1", Port: 1,
		NodeType: discovery.NodeTypeState, StateType: 3, Weight: 100}
	time.Sleep(50 * time.Millisecond)
	nodes, _ := lbi.NodeSelector.GetAllNodes("1")
	require.Len(t, nodes, 1)
	require.Equal(t, "pooled", nodes[0].Key())
}
