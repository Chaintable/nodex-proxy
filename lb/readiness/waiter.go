package readiness

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
	"github.com/Chaintable/nodex-proxy/lb/selector"
	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/Chaintable/nodex-proxy/types"
)

type ReadinessWaiter struct {
	waitingNodes map[string]context.CancelFunc
	mu           sync.RWMutex
	config       *types.Config
	nodeSelector selector.Strategy
}

func NewReadinessWaiter(config *types.Config, nodeSelector selector.Strategy) *ReadinessWaiter {
	return &ReadinessWaiter{
		waitingNodes: make(map[string]context.CancelFunc),
		config:       config,
		nodeSelector: nodeSelector,
	}
}

func (w *ReadinessWaiter) makeKey(chainId, nodeKey string) string {
	return fmt.Sprintf("%s:%s", chainId, nodeKey)
}

func (w *ReadinessWaiter) WaitForReadiness(parentCtx context.Context, tempNode *discovery.TargetNode, targetNode *lbnode.Node) {
	key := w.makeKey(tempNode.ChainId, tempNode.NodeKey)

	w.mu.Lock()
	if cancel, exists := w.waitingNodes[key]; exists {
		cancel()
	}

	ctx, cancel := context.WithTimeout(parentCtx, time.Duration(w.config.NodeReadinessTimeout)*time.Second)
	w.waitingNodes[key] = cancel
	w.mu.Unlock()

	log.Info("node readiness wait started",
		log.Any("chainId", tempNode.ChainId),
		log.Any("nodeKey", tempNode.NodeKey),
		log.Any("address", tempNode.Address),
		log.Any("readinessPort", tempNode.ReadinessPort))

	go w.waitLoop(ctx, tempNode, targetNode, key)
}

func (w *ReadinessWaiter) waitLoop(ctx context.Context, tempNode *discovery.TargetNode, targetNode *lbnode.Node, key string) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	defer func() {
		w.mu.Lock()
		delete(w.waitingNodes, key)
		w.mu.Unlock()
	}()

	checkTimeout := 5 * time.Second

	for {
		select {
		case <-ctx.Done():
			log.Info("node readiness wait cancelled or timeout",
				log.Any("chainId", tempNode.ChainId),
				log.Any("nodeKey", tempNode.NodeKey))
			return

		case <-ticker.C:
			w.mu.RLock()
			_, stillWaiting := w.waitingNodes[key]
			w.mu.RUnlock()

			if !stillWaiting {
				log.Info("node readiness wait externally cancelled",
					log.Any("chainId", tempNode.ChainId),
					log.Any("nodeKey", tempNode.NodeKey))
				return
			}

			if CheckReadiness(tempNode.Address, tempNode.ReadinessPort, w.config.ReadinessCheckPath, checkTimeout) {
				log.Info("node readiness check passed, adding to selector",
					log.Any("chainId", tempNode.ChainId),
					log.Any("nodeKey", tempNode.NodeKey),
					log.Any("address", tempNode.Address))

				_ = w.nodeSelector.UpsertNode(ctx, tempNode.ChainId, tempNode.NodeType, targetNode)

				w.mu.Lock()
				delete(w.waitingNodes, key)
				w.mu.Unlock()
				return
			}
		}
	}
}

func (w *ReadinessWaiter) CancelWait(chainId, nodeKey string) {
	key := w.makeKey(chainId, nodeKey)

	w.mu.Lock()
	defer w.mu.Unlock()

	if cancel, exists := w.waitingNodes[key]; exists {
		cancel()
		delete(w.waitingNodes, key)
		log.Info("node readiness wait cancelled",
			log.Any("chainId", chainId),
			log.Any("nodeKey", nodeKey))
	}
}
