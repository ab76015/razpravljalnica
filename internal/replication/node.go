package replication

import (
	"context"
	"fmt"
	"log"
	"sync"
	pb "github.com/ab76015/razpravljalnica/api/pb"
	"github.com/ab76015/razpravljalnica/internal/storage"
    "google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
    "google.golang.org/protobuf/types/known/timestamppb"
)

// NodeState vsebuje lokalno informacijo o sosedih in verzijo verige
type NodeState struct {
	mu          sync.RWMutex
	predecessor *pb.NodeInfo
	successor   *pb.NodeInfo
	self        *pb.NodeInfo
	head        *pb.NodeInfo
	tail        *pb.NodeInfo
	nodes       []*pb.NodeInfo
    chainVersion     uint64
    nextWriteID   uint64
}

// NewNodeState ustvari zacetno prazno stanje
func NewNodeState(self *pb.NodeInfo) *NodeState {
	return &NodeState{self: self}
}

// UpdateConfig posodobi lokalno stanje verige za vozlisce in izpise razliko
func (ns *NodeState) UpdateConfig(cfg *pb.ChainConfig) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	// snapshot previous
	var prevHeadId, prevTailId, prevPredId, prevSuccId string
	prevVersion := ns.chainVersion
	if ns.head != nil {
		prevHeadId = ns.head.NodeId
	}
	if ns.tail != nil {
		prevTailId = ns.tail.NodeId
	}
	if ns.predecessor != nil {
		prevPredId = ns.predecessor.NodeId
	}
	if ns.successor != nil {
		prevSuccId = ns.successor.NodeId
	}

	// apply new config
	ns.predecessor = cfg.Predecessor
	ns.successor = cfg.Successor
	ns.head = cfg.Head
	ns.tail = cfg.Tail
	ns.chainVersion = cfg.ChainVersion

	if len(cfg.Nodes) > 0 {
		ns.nodes = make([]*pb.NodeInfo, len(cfg.Nodes))
		copy(ns.nodes, cfg.Nodes)
	} else {
		ns.nodes = nil
	}

	// new ids
	var newHeadId, newTailId, newPredId, newSuccId string
	if ns.head != nil {
		newHeadId = ns.head.NodeId
	}
	if ns.tail != nil {
		newTailId = ns.tail.NodeId
	}
	if ns.predecessor != nil {
		newPredId = ns.predecessor.NodeId
	}
	if ns.successor != nil {
		newSuccId = ns.successor.NodeId
	}

	// log summary of changes
	log.Printf("[CHAIN-UPDATE] ChainVersion: %d -> %d | head: %s -> %s | tail: %s -> %s | pred: %s -> %s | succ: %s -> %s",
		prevVersion, ns.chainVersion,
		prevHeadId, newHeadId,
		prevTailId, newTailId,
		prevPredId, newPredId,
		prevSuccId, newSuccId,
	)

	// role transitions for this node
	selfId := ""
	if ns.self != nil {
		selfId = ns.self.NodeId
	}

	wasHead := prevHeadId != "" && selfId == prevHeadId
	isHead := newHeadId != "" && selfId == newHeadId
	if !wasHead && isHead {
		log.Printf("[ROLE] node=%s BECAME HEAD (chainver=%d)", selfId, ns.chainVersion)
	} else if wasHead && !isHead {
		log.Printf("[ROLE] node=%s LOST HEAD (chainver=%d)", selfId, ns.chainVersion)
	}

	wasTail := prevTailId != "" && selfId == prevTailId
	isTail := newTailId != "" && selfId == newTailId
	if !wasTail && isTail {
		log.Printf("[ROLE] node=%s BECAME TAIL (chainver=%d)", selfId, ns.chainVersion)
	} else if wasTail && !isTail {
		log.Printf("[ROLE] node=%s LOST TAIL (chainver=%d)", selfId, ns.chainVersion)
	}

	// pred/succ specific messages
	if prevPredId != newPredId {
		log.Printf("[NEIGHBOR] node=%s predecessor changed: %s -> %s", selfId, prevPredId, newPredId)
	}
	if prevSuccId != newSuccId {
		log.Printf("[NEIGHBOR] node=%s successor changed: %s -> %s", selfId, prevSuccId, newSuccId)
	}
}

// NodesSnapshot vrne kopijo vseh vozlišč v verigi
func (ns *NodeState) NodesSnapshot() []*pb.NodeInfo {
	ns.mu.RLock()
	defer ns.mu.RUnlock()

	if len(ns.nodes) == 0 {
		return nil
	}

	nodes := make([]*pb.NodeInfo, len(ns.nodes))
	copy(nodes, ns.nodes)
	return nodes
}

func (s *DataNodeServer) NodesSnapshot() []*pb.NodeInfo {
	return s.state.NodesSnapshot()
}


// Vrne nodeinfo vozlisca samega, ki pripada stanju ns
func (ns *NodeState) Self() *pb.NodeInfo {
    ns.mu.RLock()
    defer ns.mu.RUnlock()
    return ns.self
}

// IsHead preveri ali je vozlisce glava
func (ns *NodeState) IsHead() bool {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.self != nil && ns.head != nil && ns.self.NodeId == ns.head.NodeId
}

func (s *DataNodeServer) IsHead() bool {
	return s.state.IsHead()
}

// IsTail preveri ali je vozlisce rep
func (ns *NodeState) IsTail() bool {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.self != nil && ns.tail != nil && ns.self.NodeId == ns.tail.NodeId
}

func (s *DataNodeServer) IsTail() bool {
	return s.state.IsTail()
}

// GetState vrne trenutni config state
func (ns *NodeState) GetState() (pred, succ *pb.NodeInfo, writeID uint64) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.predecessor, ns.successor, ns.chainVersion
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

// ChainVersion vrne chainVerzijo vozlišča
func (ns *NodeState) ChainVersion() uint64 {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.chainVersion
}

// DataNodeServer implementira data node gRPC server interface (iz proto)
type DataNodeServer struct {
	pb.UnimplementedDataNodeServer
	state   *NodeState
    storage storage.Storage
	pending map[uint64]chan struct{}
	mu      sync.Mutex    
    // se poklice ko je write commitan na tem vozliscu (callbacks v bistvu)
    commitListenersMu sync.RWMutex
    commitListeners []func(ev *pb.MessageEvent)
    // za updatat nodeStatus (resync) v master
    lastCommittedMu sync.RWMutex
    lastCommitted  uint64
    // za fetchwrites
    storedWrites map[uint64]*pb.ReplicatedWrite
}

// NewDataNodeServer je konstruktor, ki sprejme NodeState in ustvari abstrakcijo streznika za verizno replikacijo
func NewDataNodeServer(state *NodeState, st storage.Storage) *DataNodeServer {
	return &DataNodeServer{
        state: state, 
        storage: st, 
        pending: make(map[uint64]chan struct{}), 
        commitListeners: make([]func(ev *pb.MessageEvent), 0), 
        storedWrites: make(map[uint64]*pb.ReplicatedWrite),
    }
}

// State vrne je getter za kazalec na stanje (NodeState) trenutnega DataNodeServer strežnika
func (s *DataNodeServer) State() *NodeState {
	return s.state
}

// UpdateChainConfig RPC ki ga klice kontrolna ravnina ko zeli posodobiti stanje streznika na podatkovni ravnini
func (s *DataNodeServer) UpdateChainConfig(ctx context.Context, cfg *pb.ChainConfig) (*emptypb.Empty, error) {
	// delegate to NodeState, which now logs details and role transitions
	s.state.UpdateConfig(cfg)

	// also log a compact view including addresses (helpful in multi-node tests)
	log.Printf("[CHAIN-UPDATE] Node self=%v | Head=%v Tail=%v | Version=%d",
		s.state.Self(),
		cfg.Head,
		cfg.Tail,
		cfg.ChainVersion,
	)

	return &emptypb.Empty{}, nil
}
	

func (s *DataNodeServer) ApplyWrite(rw *pb.ReplicatedWrite) error {
	return s.applyWrite(rw)
}

func (s *DataNodeServer) applyWrite(rw *pb.ReplicatedWrite) error {
	switch rw.Op {
        case "CreateUser":
            var req pb.CreateUserRequest
            if err := proto.Unmarshal(rw.Payload, &req); err != nil {
                return err
            }

            _, err := s.storage.CreateUser(req.Name)
            return err

        case "CreateTopic":
            var req pb.CreateTopicRequest
            if err := proto.Unmarshal(rw.Payload, &req); err != nil {
                return err
            }
            
            _, err := s.storage.CreateTopic(
                req.Name,
            )
            return err

        case "PostMessage":
            var req pb.PostMessageRequest
            if err := proto.Unmarshal(rw.Payload, &req); err != nil {
                return err
            }
            // CRAQ verzija PostMessage; sprva dirty writeID
            _, err := s.storage.PostMessageWithWriteID(
                req.TopicId,
                req.UserId,
                req.Text,
                rw.WriteId,
            )
            return err

        case "UpdateMessage":
            var req pb.UpdateMessageRequest
            if err := proto.Unmarshal(rw.Payload, &req); err != nil {
                return err
            }
            // CRAQ verzija UpdateMessage      
            _, err := s.storage.UpdateMessageWithWriteID(
                req.TopicId,
                req.UserId,
                req.MessageId,
                req.Text,
                rw.WriteId, 
            )
            return err

        case "DeleteMessage":
            var req pb.DeleteMessageRequest
            if err := proto.Unmarshal(rw.Payload, &req); err != nil {
                return err
            }
            // CRAQ
            _, err := s.storage.DeleteMessageWithWriteID(
                req.TopicId,
                req.MessageId,
                req.UserId,
                rw.WriteId,
            )
            return err

        case "LikeMessage":
            var req pb.LikeMessageRequest
            if err := proto.Unmarshal(rw.Payload, &req); err != nil {
                return err
            }
            // CRAQ
            _, err := s.storage.LikeMessageWithWriteID(
                req.TopicId,
                req.MessageId,
                req.UserId,
                rw.WriteId, 
            )
            return err

        default:
            return fmt.Errorf("unknown op %s", rw.Op)
	}
}
// OpType helper
func opTypeFromWrite(op string) pb.OpType {
    switch op {
    case "PostMessage":
        return pb.OpType_OP_POST
    case "UpdateMessage":
        return pb.OpType_OP_UPDATE
    case "LikeMessage":
        return pb.OpType_OP_LIKE
    case "DeleteMessage":
        return pb.OpType_OP_DELETE
    default:
        return pb.OpType_OP_POST
    }
}


// ReplicateFromHead LOCAL APPLY + FORWARD mora biti ena operacija
func (s *DataNodeServer) ReplicateFromHead(rw *pb.ReplicatedWrite) error {
    // apliciraj (dirty) lokalno
    if err := s.applyWrite(rw); err != nil {
        log.Printf("ReplicateFromHead: applyWrite failed: op=%s writeID=%d err=%v", rw.Op, rw.WriteId, err)
        return fmt.Errorf("applyWrite op=%s writeID=%d: %w", rw.Op, rw.WriteId, err)
    }
    s.mu.Lock()
    s.storedWrites[rw.WriteId] = rw
    s.mu.Unlock()

    log.Printf("Kot glava ustvaril zapis.\n")

    // ce ni naslednikov -> ta node je rep (en node v verigi). Oznaci commited in obvesti.
    if s.state.Successor() == nil {
        // ce je pisalna operacija znotraj topic -> poslji narocnikom
        if isMessageOp(rw.Op) {
            log.Printf("ReplicateFromHead: vozlisce je rep (single-node). Oznacili zapis %d committed.\n", rw.WriteId)
            rec, err := s.storage.MarkCommitted(rw.WriteId)
            s.lastCommittedMu.Lock()
            if rw.WriteId > s.lastCommitted {
                s.lastCommitted = rw.WriteId
            }
            s.lastCommittedMu.Unlock()
            if err == nil && rec != nil {
                ev := &pb.MessageEvent{
                    SequenceNumber: int64(rw.WriteId),
                    Op:             opTypeFromWrite(rw.Op),
                    Message: &pb.Message{
                        Id:        rec.ID,
                        TopicId:   rec.TopicID,
                        UserId:    rec.UserID,
                        Text:      rec.Text,
                        CreatedAt: timestamppb.New(rec.CreatedAt),
                        Likes:     rec.Likes,
                    },
                    EventAt: timestamppb.Now(),
                }
                s.notifyCommit(ev)
            } else if err != nil {
                log.Printf("ReplicateFromHead: MarkCommitted error: %v\n", err)
            }
        }

        // If this node is also head, there may be a pending channel waiting; emulate ack arrival
        if s.state.IsHead() {
            s.mu.Lock()
            ch, ok := s.pending[rw.WriteId]
            if ok {
                close(ch)
                delete(s.pending, rw.WriteId)
            }
            s.mu.Unlock()
        }
        return nil
    }

    // sicer poslji naslednjiku
    return s.ForwardWrite(rw)
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
func (s *DataNodeServer) sendAckBackward(writeID uint64, op string) error {
	pred := s.state.Predecessor()

    if pred == nil {
        return nil
    }

	// povezava s predhodnikom
	conn, err := grpc.Dial(pred.Address, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewDataNodeClient(conn)

	// repliciraj ack do predhodnika
	ack := &pb.ReplicatedAck{WriteId: writeID, Op: op}
	_, err = client.ReplicateAck(context.Background(), ack)

	return err
}

// isMessageOp je helper, ki pogleda ali gre za pisalno operacijo
func isMessageOp(op string) bool {
    switch op {
    case "PostMessage", "UpdateMessage", "DeleteMessage", "LikeMessage":
        return true
    default:
        return false
    }
}

// ReplicateWrite je grpc metoda (glej proto), ki pošlje rw nasledniku
func (s *DataNodeServer) ReplicateWrite(ctx context.Context, req *pb.ReplicatedWrite) (*emptypb.Empty, error) {
	// 1. apliciraj lokalno
	if err := s.applyWrite(req); err != nil {
		return nil, err
	}
    s.mu.Lock()
    s.storedWrites[req.WriteId] = req
    s.mu.Unlock()

	log.Printf("Prejel sporočilo od predhodnika in ga zapisal v lokalni storage.\n")
	if s.state.IsTail() {
        if isMessageOp(req.Op) {
            // Rep vrne ACK in obvesti svoje subscriberje o novem dogodku
            log.Printf("Kot rep: oznacil zapis %d committed in poslal ACK nazaj.\n", req.WriteId)
            rec, err := s.storage.MarkCommitted(req.WriteId)
            s.lastCommittedMu.Lock()
            if req.WriteId > s.lastCommitted {
                s.lastCommitted = req.WriteId
            }
            s.lastCommittedMu.Unlock()
            if err == nil && rec != nil {
                ev := &pb.MessageEvent{
                    SequenceNumber: int64(req.WriteId),
                    Op:             opTypeFromWrite(req.Op),
                    Message: &pb.Message{
                        Id:        rec.ID,
                        TopicId:   rec.TopicID,
                        UserId:    rec.UserID,
                        Text:      rec.Text,
                        CreatedAt: timestamppb.New(rec.CreatedAt),
                        Likes:     rec.Likes,
                    },
                    EventAt: timestamppb.Now(),
                }
                s.notifyCommit(ev)
            } else if err != nil {
                log.Printf("Rep: MarkCommitted error: %v\n", err)
            }
        }
		s.sendAckBackward(req.WriteId, req.Op)
		return &emptypb.Empty{}, nil
	}

	log.Printf("Poslal sporočilo naprej nasledniku.\n")
	// 2. Pošlji naslednjiku
	s.ForwardWrite(req)
	return &emptypb.Empty{}, nil
}

// ReplicateAck je grpc metoda (glej proto), ki pošlje ra predhodniku
func (s *DataNodeServer) ReplicateAck(ctx context.Context, req *pb.ReplicatedAck) (*emptypb.Empty, error) {
	// Oznaci kot commited lokalno in obvesti subscriberje
    if isMessageOp(req.Op) {
        rec, err := s.storage.MarkCommitted(req.WriteId)
        s.lastCommittedMu.Lock()
        if req.WriteId > s.lastCommitted {
            s.lastCommitted = req.WriteId
        }
        s.lastCommittedMu.Unlock()
        if err == nil && rec != nil {
            ev := &pb.MessageEvent{
                SequenceNumber: int64(req.WriteId),
                Op:             opTypeFromWrite(req.Op),
                Message: &pb.Message{
                    Id:        rec.ID,
                    TopicId:   rec.TopicID,
                    UserId:    rec.UserID,
                    Text:      rec.Text,
                    CreatedAt: timestamppb.New(rec.CreatedAt),
                    Likes:     rec.Likes,
                },
                EventAt: timestamppb.Now(),
            }
            s.notifyCommit(ev)
        } else if err != nil {
            // not fatal: morda nismo imeli zapisa lokalno (log and continue)
            log.Printf("warning, ReplicateAck: MarkCommitted error for write %d: %v\n", req.WriteId, err)
        }
    }

    if s.state.IsHead() {
		// ACK je prišel do glave
		s.mu.Lock()
		channel, ok := s.pending[req.WriteId]
		if ok {
			close(channel)
			delete(s.pending, req.WriteId)
		}
		s.mu.Unlock()
		log.Printf("Kot glava sprejel zadnji ACK.\n")
		return &emptypb.Empty{}, nil
	}
	log.Printf("Poslal ACK predhodniku.\n")
	// Forward ack backward
    if err := s.sendAckBackward(req.WriteId, req.Op); err != nil {
        log.Printf("ACK forward failed: %v", err)
    }
	return &emptypb.Empty{}, nil
}

// Registrira čakajoči ACK
func (s *DataNodeServer) RegisterPendingACK(writeID uint64) chan struct{} {
	ch := make(chan struct{})
	s.mu.Lock()
	s.pending[writeID] = ch
	s.mu.Unlock()
	return ch
}

// Zbriše čakajoči ACK (zapre ga lahko le ACK receiver)
func (s *DataNodeServer) CancelPendingACK(writeID uint64) {
    s.mu.Lock()
    delete(s.pending, writeID)
    s.mu.Unlock()
}

func (ns *NodeState) NextWriteID() uint64 {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.nextWriteID++
	return ns.nextWriteID
}

// Commit listener upravljanje; doda callback v rezino; poveze server.go z node.go
func (s *DataNodeServer) RegisterCommitListener(fn func(ev *pb.MessageEvent)) {
    s.commitListenersMu.Lock()
    s.commitListeners = append(s.commitListeners, fn)
    s.commitListenersMu.Unlock()
}

// Vsakic ko se zgodi commit obvesti subscriberje
func (s *DataNodeServer) notifyCommit(ev *pb.MessageEvent) {
    s.commitListenersMu.RLock()
    listeners := make([]func(ev *pb.MessageEvent), len(s.commitListeners))
    copy(listeners, s.commitListeners)
    s.commitListenersMu.RUnlock()

    for _, fn := range listeners {
        // poklici async da ne blokiramo replikacije
        go func(f func(ev *pb.MessageEvent)) {
            defer func() {
                if r := recover(); r != nil {
                    log.Printf("commit listener panicked: %v", r)
                }
            }()
            // med drugim poklice emitEvent iz server.go, ki poslje vsem subscriberjem
            f(ev)
        }(fn)
    }
}

// implementacija heartbeat, vrne lastcommited msg na node in nodeid ter chainversion
func (s *DataNodeServer) Heartbeat(ctx context.Context, _ *pb.HeartbeatReq) (*pb.HeartbeatResp, error) {
    s.lastCommittedMu.RLock()
    lc := s.lastCommitted
    s.lastCommittedMu.RUnlock()
    return &pb.HeartbeatResp{
        LastCommittedWrite: lc,
        NodeId: s.state.Self().NodeId,
        ChainVersion: s.state.ChainVersion(),
    }, nil
}

// FetchWrites vrne sporocila od from do to, masterju
func (s *DataNodeServer) FetchWrites(req *pb.FetchWritesReq, stream pb.DataNode_FetchWritesServer) error {
    from := req.FromWriteId
    to := req.ToWriteId // 0 means until latest
    // iterate over some storage map that keeps writeID->ReplicatedWrite or messages
    // You need to have stored the original ReplicatedWrite per writeID; if not, reconstruct minimal ReplicatedWrite
    s.mu.Lock()
    // if you have s.storedWrites map[uint64]*pb.ReplicatedWrite use that
    // fallback: reconstruct from m.writes or msgWrite. For simplicity, iterate over available writeIDs
    var max uint64
    for w := range s.storedWrites { if w > max { max = w } }
    s.mu.Unlock()

    if to == 0 || to > max+1 {
        to = max + 1
    }
    if from >= to {
        return nil
    }
    for wid := from; wid < to; wid++ {
        s.mu.Lock()
        rw, ok := s.storedWrites[wid]
        s.mu.Unlock()
        if !ok {
            continue
        }
        if err := stream.Send(rw); err != nil {
            return err
        }
    }
    return nil
}

