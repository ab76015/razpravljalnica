package control

import (
    "sync"
    pb "github.com/ab76015/razpravljalnica/api/pb"   
)

// ChainState hrani stanje verige
type ChainState struct {
    mu sync.RWMutex
    Head *pb.NodeInfo
    Tail *pb.NodeInfo
    Middles []*pb.NodeInfo
}

// NewChainState ustvari začetno stanje verige
func NewChainState(head, tail *pb.NodeInfo, middles []*pb.NodeInfo) *ChainState {
    return &ChainState{
        Head: head,
        Tail: tail,
        Middles: middles,
    }
}

// GetClusterState vrne stanje verige; uporabimo RW kljucavnico da vec bralcev hkrati bere; ni grpc metoda
func (cs *ChainState) GetClusterState() (*pb.NodeInfo, *pb.NodeInfo, []*pb.NodeInfo) {
    cs.mu.RLock()
    defer cs.mu.RUnlock()

    //kopije da ne pride do race condition
    head := &pb.NodeInfo{
        NodeId: cs.Head.NodeId, 
        Address: cs.Head.Address,
    }
    tail := &pb.NodeInfo{
        NodeId: cs.Tail.NodeId, 
        Address: cs.Tail.Address, 
    }
    middles := make([]*pb.NodeInfo, len(cs.Middles))
    for i, m := range cs.Middles {
        middles[i] = &pb.NodeInfo{
            NodeId: m.NodeId,
            Address: m.Address,
        }
    }
    return head, tail, middles
}

// SetChain posodobi celo stanje verige atomarno; bo klical master v nadzorni ravnini; ni grpc metoda
func (cs *ChainState) SetChain(head *pb.NodeInfo, tail *pb.NodeInfo, middles []*pb.NodeInfo) {
    cs.mu.Lock()
    defer cs.mu.Unlock()
    cs.Head = head
    cs.Tail = tail
    cs.Middles = middles
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

