package lb

import "sync"

// DownstreamManager keeps mapping between parent chains and their downstream chains.
type DownstreamManager struct {
	mu               sync.RWMutex
	parentToChildren map[string]map[string]struct{}
	childToParents   map[string]map[string]struct{}
}

func NewDownstreamManager() *DownstreamManager {
	return &DownstreamManager{
		parentToChildren: make(map[string]map[string]struct{}),
		childToParents:   make(map[string]map[string]struct{}),
	}
}

// Add registers a downstream mapping and returns true if it is newly added.
func (m *DownstreamManager) Add(parent, child string) bool {
	if parent == "" || child == "" {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	children, ok := m.parentToChildren[parent]
	if !ok {
		children = make(map[string]struct{})
		m.parentToChildren[parent] = children
	}
	if _, exists := children[child]; exists {
		return false
	}
	children[child] = struct{}{}

	parents, ok := m.childToParents[child]
	if !ok {
		parents = make(map[string]struct{})
		m.childToParents[child] = parents
	}
	parents[parent] = struct{}{}
	return true
}

// Remove unregisters a downstream mapping and returns true if a mapping was removed.
func (m *DownstreamManager) Remove(parent, child string) bool {
	if parent == "" || child == "" {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	children, ok := m.parentToChildren[parent]
	if !ok {
		return false
	}
	if _, exists := children[child]; !exists {
		return false
	}
	delete(children, child)
	if len(children) == 0 {
		delete(m.parentToChildren, parent)
	}

	parents := m.childToParents[child]
	delete(parents, parent)
	if len(parents) == 0 {
		delete(m.childToParents, child)
	}
	return true
}

func (m *DownstreamManager) Parents(child string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	parentsMap, ok := m.childToParents[child]
	if !ok {
		return nil
	}

	parents := make([]string, 0, len(parentsMap))
	for parent := range parentsMap {
		parents = append(parents, parent)
	}
	return parents
}

func (m *DownstreamManager) Children(parent string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	childrenMap, ok := m.parentToChildren[parent]
	if !ok {
		return nil
	}

	children := make([]string, 0, len(childrenMap))
	for child := range childrenMap {
		children = append(children, child)
	}
	return children
}
