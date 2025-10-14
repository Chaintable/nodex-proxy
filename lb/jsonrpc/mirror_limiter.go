package jsonrpc

import (
	"fmt"
	"sync"

	"golang.org/x/time/rate"
)

type MirrorLimiter interface {
	Allow(chainId, mirrorURL string) bool
	UpdateLimit(chainId, mirrorURL string, rps int) bool
	RemoveLimit(chainId, mirrorURL string)
}

type mirrorLimiter struct {
	limiters map[string]*rate.Limiter
	sync.RWMutex
}

func NewMirrorLimiter() MirrorLimiter {
	return &mirrorLimiter{
		limiters: make(map[string]*rate.Limiter),
	}
}

func (ml *mirrorLimiter) makeKey(chainId, mirrorURL string) string {
	return fmt.Sprintf("%s:%s", chainId, mirrorURL)
}

func (ml *mirrorLimiter) UpdateLimit(chainId, mirrorURL string, rps int) bool {
	if chainId == "" || mirrorURL == "" || rps < 0 {
		return false
	}

	key := ml.makeKey(chainId, mirrorURL)
	ml.Lock()
	defer ml.Unlock()

	if rps == 0 {
		delete(ml.limiters, key)
	} else {
		ml.limiters[key] = rate.NewLimiter(rate.Limit(rps), rps)
	}
	return true
}

func (ml *mirrorLimiter) RemoveLimit(chainId, mirrorURL string) {
	key := ml.makeKey(chainId, mirrorURL)
	ml.Lock()
	delete(ml.limiters, key)
	ml.Unlock()
}

func (ml *mirrorLimiter) Allow(chainId, mirrorURL string) bool {
	key := ml.makeKey(chainId, mirrorURL)
	ml.RLock()
	limiter, exists := ml.limiters[key]
	ml.RUnlock()

	if !exists {
		return true
	}
	return limiter.Allow()
}
