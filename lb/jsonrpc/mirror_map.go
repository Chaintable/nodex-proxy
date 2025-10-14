package jsonrpc

import (
	"sync"

	"github.com/Chaintable/nodex-proxy/discovery"
)

type MirrorMap interface {
	AddMirrorTarget(chainId string, addrKey string, target *discovery.MirrorTarget)
	DeleteMirrorTarget(chainId string, addrKey string)
	GetMirrorTargets(chainId string) []*discovery.MirrorTarget
	GetMirrorURLs(chainId string) []string
	GetAllMirrors() map[string][]*discovery.MirrorTarget
	DeleteChainMirrors(chainId string)
}

type mirrorMap struct {
	mirrors map[string]map[string]*discovery.MirrorTarget
	sync.RWMutex
}

func NewMirrorMap() MirrorMap {
	return &mirrorMap{
		mirrors: make(map[string]map[string]*discovery.MirrorTarget),
	}
}

func (m *mirrorMap) AddMirrorTarget(chainId string, addrKey string, target *discovery.MirrorTarget) {
	m.Lock()
	defer m.Unlock()

	if m.mirrors[chainId] == nil {
		m.mirrors[chainId] = make(map[string]*discovery.MirrorTarget)
	}

	m.mirrors[chainId][addrKey] = target
}

func (m *mirrorMap) DeleteMirrorTarget(chainId string, addrKey string) {
	m.Lock()
	defer m.Unlock()

	if m.mirrors[chainId] != nil {
		delete(m.mirrors[chainId], addrKey)

		if len(m.mirrors[chainId]) == 0 {
			delete(m.mirrors, chainId)
		}
	}
}

func (m *mirrorMap) GetMirrorTargets(chainId string) []*discovery.MirrorTarget {
	m.RLock()
	defer m.RUnlock()

	targets := []*discovery.MirrorTarget{}
	if m.mirrors[chainId] != nil {
		for _, target := range m.mirrors[chainId] {
			targets = append(targets, target)
		}
	}

	return targets
}

func (m *mirrorMap) GetMirrorURLs(chainId string) []string {
	m.RLock()
	defer m.RUnlock()

	urls := []string{}
	if m.mirrors[chainId] != nil {
		for _, target := range m.mirrors[chainId] {
			urls = append(urls, target.URL())
		}
	}

	return urls
}

func (m *mirrorMap) DeleteChainMirrors(chainId string) {
	m.Lock()
	defer m.Unlock()

	delete(m.mirrors, chainId)
}

func (m *mirrorMap) GetAllMirrors() map[string][]*discovery.MirrorTarget {
	m.RLock()
	defer m.RUnlock()

	result := make(map[string][]*discovery.MirrorTarget)
	for chainId, targetMap := range m.mirrors {
		if len(targetMap) > 0 {
			targets := make([]*discovery.MirrorTarget, 0, len(targetMap))
			for _, target := range targetMap {
				targets = append(targets, target)
			}
			result[chainId] = targets
		}
	}

	return result
}
