package replication

import (
    "log"
    "context"
    "sync"
    pb "github.com/ab76015/razpravljalnica/api/pb"
    "google.golang.org/protobuf/types/known/emptypb"
)

// NodeState vsebuje lokalno informacijo o sosedih in verzijo verige
type NodeState struct {
    mu          sync.RWMutex
    predecessor *pb.NodeInfo
    successor   *pb.NodeInfo
    self        *pb.NodeInfo
    head        *pb.NodeInfo
    tail        *pb.NodeInfo
    version     uint64
}

// NewNodeState ustvari zacetno prazno stanje
func NewNodeState(self *pb.NodeInfo) *NodeState {
    return &NodeState{self: self}
}

// UpdateConfig posodobi lokalno stanje verige za vozlisce
func (ns *NodeState) UpdateConfig(cfg *pb.ChainConfig) {
    ns.mu.Lock()
    defer ns.mu.Unlock()
    ns.predecessor = cfg.Predecessor
    ns.successor = cfg.Successor
    ns.head = cfg.Head
    ns.tail = cfg.Tail
    ns.version = cfg.Version
}

// IsHead preveri ali je vozlisce glava
func (ns *NodeState) IsHead() bool {
    ns.mu.RLock()
    defer ns.mu.RUnlock()
    return ns.self != nil && ns.head != nil && ns.self.NodeId == ns.head.NodeId
}

// IsTail preveri ali je vozlisce rep
func (ns *NodeState) IsTail() bool {
    ns.mu.RLock()
    defer ns.mu.RUnlock()
    return ns.self != nil && ns.tail != nil && ns.self.NodeId == ns.tail.NodeId
}

// GetState vrne trenutni config state
func (ns *NodeState) GetState() (pred, succ *pb.NodeInfo, version uint64) {
    ns.mu.RLock()
    defer ns.mu.RUnlock()
    return ns.predecessor, ns.successor, ns.version
}

// DataNodeServer implementira data node gRPC server interface (iz proto)
type DataNodeServer struct {
    pb.UnimplementedDataNodeServer
    state *NodeState
}

func NewDataNodeServer(state *NodeState) *DataNodeServer {
    return &DataNodeServer{state: state}
}

// UpdateChainConfig RPC ki ga klice kontrolna ravnina ko zeli posodobiti stanje streznika na podatkovni ravnini
func (s *DataNodeServer) UpdateChainConfig(ctx context.Context, cfg *pb.ChainConfig) (*emptypb.Empty, error) {
    s.state.UpdateConfig(cfg)
    log.Printf(
    "[CHAIN-UPDATE] version=%d pred=(%v) succ=(%v)\n head=(%v) tail=(%v)\n",
    cfg.Version,
    cfg.Predecessor,
    cfg.Successor,
    cfg.Head,
    cfg.Tail,
    )
    return &emptypb.Empty{}, nil
}

