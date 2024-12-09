package random

import (
	"context"

	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/utils"
)

type Random struct {
}

func New() *Random {
	return &Random{}
}

func (r *Random) GetNode(_ context.Context, nodes []*lbnode.Node, _ string) (*lbnode.Node, error) {
	var tempNodes []*lbnode.Node
	weightSum := 0
	for _, node := range nodes {
		tempNodes = append(tempNodes, node)
		weightSum += node.EffectWeight()
	}

	if len(tempNodes) == 0 {
		return nil, utils.ErrNoAvailableNode
	}
	returnWeight := utils.RangeRandom(0, int64(weightSum))
	tempWeight := 0
	for _, node := range tempNodes {
		tempWeight += node.EffectWeight()
		if int64(tempWeight) >= returnWeight {
			finalNode := node.Clone()
			return finalNode, nil
		}
	}

	return nil, utils.ErrNoAvailableNode
}

func (r *Random) String() string {
	return "Random"
}
