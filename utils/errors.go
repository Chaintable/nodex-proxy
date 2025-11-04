package utils

import (
	"errors"
	"fmt"
)

var ErrNoAvailableNode = errors.New("no available node")

// NoAvailableNodeError 表示没有可用节点的详细错误
type NoAvailableNodeError struct {
	ChainId string
	Reason  string
}

func (e *NoAvailableNodeError) Error() string {
	if e.ChainId != "" {
		return fmt.Sprintf("no available node for chain %s: %s", e.ChainId, e.Reason)
	}
	return fmt.Sprintf("no available node: %s", e.Reason)
}

// Is 实现 errors.Is 接口，使其与 ErrNoAvailableNode 兼容
func (e *NoAvailableNodeError) Is(target error) bool {
	return target == ErrNoAvailableNode
}

// NewNoAvailableNodeError 创建一个新的 NoAvailableNodeError
func NewNoAvailableNodeError(chainId, reason string) *NoAvailableNodeError {
	return &NoAvailableNodeError{
		ChainId: chainId,
		Reason:  reason,
	}
}
