package selector

import (
	"context"
	"fmt"

	"github.com/Chaintable/nodex-proxy/lb/lbnode"
)

// ErrNoAvailableNode ...

type Strategy interface {
	fmt.Stringer

	GetNode(ctx context.Context, nodes []*lbnode.Node, requestKey string) (*lbnode.Node, error)
}
