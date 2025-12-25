package selector

import (
	"context"
	"fmt"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// ErrNoAvailableNode ...

type Strategy interface {
	fmt.Stringer

	GetNode(ctx *types.RequestContext, requestKey string) (*lbnode.Node, error)
	UpsertNode(ctx context.Context, chainId string, role discovery.NodeType, node *lbnode.Node) error
	RemoveNode(ctx context.Context, chainId string, role discovery.NodeType, node *lbnode.Node) error
	UpdateChainHeight(ctx context.Context, chainId string, chainHeight *hexutil.Big) error

	// New methods for weight management
	GetAllNodes(chainId string) ([]*lbnode.Node, bool)
	GetArchiveNodes(chainId string) ([]*lbnode.Node, bool)
	GetStateNodes(chainId string) ([]*lbnode.Node, bool)
	GetNativeNodes(chainId string) ([]*lbnode.Node, bool)
	GetAllChainsIDs() []string
}
