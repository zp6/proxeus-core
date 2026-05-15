package proxeus

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// NodeStatePersister handles persisting external node state
// Addresses issue #146: External node loses state if abandoned

type NodeState struct {
	NodeID      string                 `json:"node_id"`
	WorkflowID  string                 `json:"workflow_id"`
	Inputs      map[string]interface{} `json:"inputs"`
	Outputs     map[string]interface{} `json:"outputs"`
	Status      string                 `json:"status"`
	LastUpdated time.Time              `json:"last_updated"`
}

type NodeStatePersister struct {
	mu       sync.RWMutex
	stateDir string
	states   map[string]*NodeState
}

func NewNodeStatePersister(stateDir string) *NodeStatePersister {
	return &NodeStatePersister{stateDir: stateDir, states: make(map[string]*NodeState)}
}

func (p *NodeStatePersister) SaveState(state *NodeState) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	state.LastUpdated = time.Now()
	p.states[state.NodeID] = state
	filePath := filepath.Join(p.stateDir, state.NodeID+".json")
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil { return err }
	return os.WriteFile(filePath, data, 0644)
}

func (p *NodeStatePersister) LoadState(nodeID string) (*NodeState, error) {
	p.mu.RLock()
	if state, ok := p.states[nodeID]; ok {
		p.mu.RUnlock()
		return state, nil
	}
	p.mu.RUnlock()
	filePath := filepath.Join(p.stateDir, nodeID+".json")
	data, err := os.ReadFile(filePath)
	if err != nil { return nil, err }
	var state NodeState
	if err := json.Unmarshal(data, &state); err != nil { return nil, err }
	p.mu.Lock()
	p.states[nodeID] = &state
	p.mu.Unlock()
	return &state, nil
}

func (p *NodeStatePersister) RecoverAbandoned(maxAge time.Duration) []*NodeState {
	p.mu.Lock()
	defer p.mu.Unlock()
	var recovered []*NodeState
	now := time.Now()
	for id, state := range p.states {
		if state.Status == "running" && now.Sub(state.LastUpdated) > maxAge {
			state.Status = "abandoned"
			recovered = append(recovered, state)
		}
	}
	return recovered
}
