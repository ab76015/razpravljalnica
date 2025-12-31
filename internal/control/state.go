package control

import (
    "log"
    "sync"
    pb "github.com/ab76015/razpravljalnica/api/pb"   
)

// ChainState hrani stanje verige
type ChainState struct {
    mu     sync.RWMutex
    version  uint64
    nodes  []*pb.NodeInfo // urejeno kot: [head, ..., tail]
}

// NewChainState ustvari začetno stanje verige
func NewChainState() *ChainState {
    return &ChainState{
        nodes: make([]*pb.NodeInfo, 0),
        version:  0,
    }
}

// NodesSnapshot vrne kopijo vozlisc in verzijo verige
func (cs *ChainState) NodesSnapshot() ([]*pb.NodeInfo, uint64) {
    cs.mu.RLock()
    defer cs.mu.RUnlock()

    nodes := make([]*pb.NodeInfo, len(cs.nodes))
    copy(nodes, cs.nodes)
    return nodes, cs.version
}

// Snapshot vrne glavo, rep in verzijo verige
func (cs *ChainState) Snapshot() (head, tail *pb.NodeInfo, version uint64) {
    cs.mu.RLock()
    defer cs.mu.RUnlock()

    if len(cs.nodes) == 0 {
        return nil, nil, cs.version
    }

    return cs.nodes[0], cs.nodes[len(cs.nodes)-1], cs.version
}

// AddNode doda vozlisce na rep in vrne njegov index
func (cs *ChainState) AddNode(node *pb.NodeInfo) int {
    cs.mu.Lock()
    defer cs.mu.Unlock()

    cs.nodes = append(cs.nodes, node)
    cs.version++
    log.Printf("[STATE-UPDATE] added node_id=%s\n", node.NodeId)
    return len(cs.nodes) - 1
}

/*
 326 type NodeInfo struct {
 327     state         protoimpl.MessageState `protogen:"open.v1"`
 328     NodeId        string                 `protobuf:"bytes,1,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
 329     Address       string                 `protobuf:"bytes,2,opt,name=address,proto3" json:"address,omitempty"`
 330     unknownFields protoimpl.UnknownFields
 331     sizeCache     protoimpl.SizeCache
 332 }
*/

