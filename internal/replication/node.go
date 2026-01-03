package replication

import (
	"context"
	"fmt"
	"log"
	"sync"

	pb "github.com/ab76015/razpravljalnica/api/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
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

// Successor vrne naslednika vozlišča
func (ns *NodeState) Successor() *pb.NodeInfo {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.successor
}

// Predecessor vrne predhodnika vozlišča
func (ns *NodeState) Predecessor() *pb.NodeInfo {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.predecessor
}

// Version vrne verzijo vozlišča
func (ns *NodeState) Version() uint64 {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.version
}

// DataNodeServer implementira data node gRPC server interface (iz proto)
type DataNodeServer struct {
	pb.UnimplementedDataNodeServer
	state   *NodeState
	pending map[uint64]chan struct{}
	mu      sync.Mutex
}

// NewDataNodeServer je konstruktor, ki sprejme NodeState in ustvari abstrakcijo streznika za verizno replikacijo
func NewDataNodeServer(state *NodeState) *DataNodeServer {
	return &DataNodeServer{state: state}
}

// State vrne je getter za kazalec na stanje (NodeState) trenutnega DataNodeServer strežnika
func (s *DataNodeServer) State() *NodeState {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	return s.state
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

func (s *DataNodeServer) applyWrite(rw *pb.ReplicatedWrite) error {
	switch rw.Op {

	case "PostMessage":
		var req pb.PostMessageRequest
		if err := proto.Unmarshal(rw.Payload, &req); err != nil {
			return err
		}

		return s.storage.PostMessage(&req)

	case "CreateUser":
		var req pb.CreateUserRequest
		if err := proto.Unmarshal(rw.Payload, &req); err != nil {
			return err
		}

		return s.storage.CreateUser(&req)

	case "CreateTopic":
		var req pb.CreateTopicRequest
		if err := proto.Unmarshal(rw.Payload, &req); err != nil {
			return err
		}
		return s.storage.CreateTopic(&req)

	case "UpdateMessage":
		var req pb.UpdateMessageRequest
		if err := proto.Unmarshal(rw.Payload, &req); err != nil {
			return err
		}
		return s.storage.UpdateMessage(&req)

	case "DeleteMessage":
		var req pb.DeleteMessageRequest
		if err := proto.Unmarshal(rw.Payload, &req); err != nil {
			return err
		}
		return s.storage.DeleteMessage(&req)

	case "LikeMessage":
		var req pb.LikeMessageRequest
		if err := proto.Unmarshal(rw.Payload, &req); err != nil {
			return err
		}
		return s.storage.LikeMessage(&req)
	// itd.. ...TODO

	default:
		return fmt.Errorf("unknown op %s", rw.Op)
	}
}

// ForwardWrite vzpostavi povezavo z succ in mu pošlje rw
func (s *DataNodeServer) ForwardWrite(rw *pb.ReplicatedWrite) error {
	succ := s.state.Successor()
	if succ == nil {
		return nil // tail, nothing to forward
	}

	conn, err := grpc.Dial(succ.Address, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewDataNodeClient(conn)

	_, err = client.ReplicateWrite(context.Background(), rw)

	return err
}

// sendAckBackward vzpostavi povezavo z pred in mu pošlje ack
func (s *DataNodeServer) sendAckBackward(version uint64) error {
	pred := s.state.Predecessor()

	// povezava s predhodnikom
	conn, err := grpc.Dial(pred.Address, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewDataNodeClient(conn)

	// repliciraj ack do predhodnika
	ack := &pb.ReplicatedAck{Version: version}
	_, err = client.ReplicateAck(context.Background(), ack)

	return err
}

// ReplicateWrite je grpc metoda (glej proto), ki pošlje rw nasledniku
func (s *DataNodeServer) ReplicateWrite(ctx context.Context, req *pb.ReplicatedWrite) (*emptypb.Empty, error) {
	// 1. apliciraj lokalno
	if err := s.applyWrite(req); err != nil {
		return nil, err
	}

	log.Printf("Prejel sporočilo od predhodnika in ga zapisal v lokalni storage.\n")
	if s.state.IsTail() {
		// Rep vrne ACK
		log.Printf("Kot rep poslal ACK predhodniku.")
		s.sendAckBackward(req.Version)
		return &emptypb.Empty{}, nil
	}

	log.Printf("Poslal sporočilo naprej nasledniku.\n")
	// 2. Pošlji naslednjiku
	s.ForwardWrite(req)
	return &emptypb.Empty{}, nil
}

// ReplicateAck je grpc metoda (glej proto), ki pošlje ra predhodniku
func (s *DataNodeServer) ReplicateAck(ctx context.Context, req *pb.ReplicatedAck) (*emptypb.Empty, error) {
	if s.state.IsHead() {
		// ACK je prišel do glave
		s.mu.Lock()
		channel, ok := s.pending[req.Version]
		if ok {
			close(channel)
			delete(s.pending, req.Version)
		}
		s.mu.Unlock()
		log.Printf("Kot glava sprejel zadnji ACK.\n")
		return &emptypb.Empty{}, nil
	}
	log.Printf("Poslal ACK predhodniku.\n")
	// Forward ack backward
	s.sendAckBackward(req.Version)
	return &emptypb.Empty{}, nil
}
