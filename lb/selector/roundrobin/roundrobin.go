package roundrobin

import (
	"context"

	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/utils"
)

type RoundRobin struct {
}

func New() *RoundRobin {
	return &RoundRobin{}
}

func (r *RoundRobin) GetNode(_ context.Context, nodes []*lbnode.Node, requestKey string) (*lbnode.Node, error) {
	var best *lbnode.Node
	total := 0

	for _, node := range nodes {
		node.IncrCurrentWeight(node.EffectWeight())
		total += node.EffectWeight()

		if best == nil || node.CurrentWeight() > best.CurrentWeight() {
			best = node
		}

	}

	if best == nil {
		return nil, utils.ErrNoAvailableNode
	}

	best.IncrCurrentWeight(total * -1)

	node := best.Clone()
	return node, nil
}
func (r *RoundRobin) String() string {
	return "Weighted Round Robin"
}
