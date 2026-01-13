package control

import (
    "context"
    "log"
    "time"
    "sync"
    "google.golang.org/grpc"
    pb "github.com/ab76015/razpravljalnica/api/pb"
    "google.golang.org/protobuf/types/known/emptypb"
)

type ControlServer struct {
    pb.UnimplementedControlPlaneServer
    state *ChainState
    // zdravje
    hbInterval time.Duration
    hbTimeout  time.Duration

    stopCh chan struct{}
    // resync
    statusMu sync.Mutex
    status   map[string]*nodeStatus
    // failure detection
    missMu       sync.Mutex
    missCount    map[string]int
    missThreshold int
}

// za resync
type nodeStatus struct {
    lastCommitted uint64
    lastSeen      time.Time
}

func NewControlServer(state *ChainState) *ControlServer {
    return &ControlServer{
        state: state,
        hbInterval: 3 * time.Second,
        hbTimeout:  2 * time.Second,
        stopCh: make(chan struct{}),
        status: make(map[string]*nodeStatus),
        missCount: make(map[string]int),
        missThreshold: 3, // require 3 misses before declaring failure
    }
}

// StartMonitor se klice iz cmd main.go 
func (s *ControlServer) StartMonitor() {
    go s.monitorLoop()
}

func (s *ControlServer) StopMonitor() {
    close(s.stopCh)
}

func (s *ControlServer) monitorLoop() {
    ticker := time.NewTicker(s.hbInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            s.checkAllNodes()
        case <-s.stopCh:
            return
        }
    }
}

// checkAllNodes vsake 3s izvede heartbeat in preveri verigo
func (s *ControlServer) checkAllNodes() {
    nodes, _ := s.state.NodesSnapshot()
    for _, n := range nodes {
        go func(node *pb.NodeInfo) {
            ctx, cancel := context.WithTimeout(context.Background(), s.hbTimeout)
            defer cancel()

            conn, err := grpc.DialContext(ctx, node.Address, grpc.WithInsecure(), grpc.WithBlock())
            if err != nil {
                log.Printf("[HB] node %s unreachable: %v", node.NodeId, err)

                // increment miss counter
                s.missMu.Lock()
                s.missCount[node.NodeId]++
                misses := s.missCount[node.NodeId]
                s.missMu.Unlock()

                if misses >= s.missThreshold {
                    log.Printf("[HB] node %s missed %d heartbeats -> declaring failed", node.NodeId, misses)
                    s.handleNodeFailure(node, "unreachable")
                } else {
                    log.Printf("[HB] node %s missed %d/%d heartbeats; will wait", node.NodeId, misses, s.missThreshold)
                }
                return
            }
            defer conn.Close()

            client := pb.NewDataNodeClient(conn)
            resp, err := client.Heartbeat(ctx, &pb.HeartbeatReq{})
            if err != nil {
                log.Printf("[HB] heartbeat failed for %s: %v", node.NodeId, err)
                // increment miss counter on heartbeat failure too
                s.missMu.Lock()
                s.missCount[node.NodeId]++
                misses := s.missCount[node.NodeId]
                s.missMu.Unlock()

                if misses >= s.missThreshold {
                    log.Printf("[HB] node %s missed %d heartbeats -> declaring failed", node.NodeId, misses)
                    s.handleNodeFailure(node, "hb-fail")
                } else {
                    log.Printf("[HB] node %s missed %d/%d heartbeats; will wait", node.NodeId, misses, s.missThreshold)
                }
                return
            }

            // successful heartbeat -> reset miss counter and update status
            s.missMu.Lock()
            s.missCount[node.NodeId] = 0
            s.missMu.Unlock()

            s.statusMu.Lock()
            if s.status == nil {
                s.status = map[string]*nodeStatus{}
            }
            s.status[node.NodeId] = &nodeStatus{lastCommitted: resp.LastCommittedWrite, lastSeen: time.Now()}
            s.statusMu.Unlock()

            log.Printf("[HB] node %s alive last_committed=%d chainver=%d", resp.NodeId, resp.LastCommittedWrite, resp.ChainVersion)
        }(n)
    }
}

// syncRangeBetween uskladi zapise me pred in succ
func (s *ControlServer) syncRangeBetween(pred, succ *pb.NodeInfo) {
    s.statusMu.Lock()
    predStatus := s.status[pred.NodeId]
    succStatus := s.status[succ.NodeId]
    s.statusMu.Unlock()

    if predStatus == nil || succStatus == nil {
        log.Printf("[SYNC] missing status for pred or succ; skipping")
        return
    }

    from := succStatus.lastCommitted + 1
    to := predStatus.lastCommitted + 1 // exclusive
    if from >= to {
        log.Printf("[SYNC] nothing to sync pred=%d succ=%d", predStatus.lastCommitted, succStatus.lastCommitted)
        return
    }

    // Poklici predhodnika in zahtevaj zapise
    connPred, err := grpc.Dial(pred.Address, grpc.WithInsecure())
    if err != nil {
        log.Printf("[SYNC] failed to dial pred %s: %v", pred.NodeId, err)
        return
    }
    defer connPred.Close()
    predClient := pb.NewDataNodeClient(connPred)

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    stream, err := predClient.FetchWrites(ctx, &pb.FetchWritesReq{FromWriteId: from, ToWriteId: to})
    if err != nil {
        log.Printf("[SYNC] FetchWrites from pred failed: %v", err)
        return
    }

    // Poklici naslednika in preposlji mu zapise r ReplicateWrite RPC
    connSucc, err := grpc.Dial(succ.Address, grpc.WithInsecure())
    if err != nil {
        log.Printf("[SYNC] failed to dial succ %s: %v", succ.NodeId, err)
        return
    }
    defer connSucc.Close()
    succClient := pb.NewDataNodeClient(connSucc)

    for {
        rw, err := stream.Recv()
        if err != nil {
            // stream ended or error
            break
        }
        // poslji succ
        if _, err := succClient.ReplicateWrite(context.Background(), rw); err != nil {
            log.Printf("[SYNC] forward to succ failed: %v", err)
            return
        }
    }

    log.Printf("[SYNC] done syncing writes %d..%d from %s -> %s", from, to-1, pred.NodeId, succ.NodeId)
}


// handleNodeFailure 
func (s *ControlServer) handleNodeFailure(node *pb.NodeInfo, reason string) {
    idx := s.state.RemoveNode(node.NodeId)
    if idx == -1 {
        return
    }

    // obvesti vsa vozlisca o novem stanju verige
    s.notifyAllNodes()

    // ce odstanjen node ni bil head=tail je treba verigo popraviti
    nodes, _ := s.state.NodesSnapshot()
    if len(nodes) == 0 {
        return
    }

    // ce je bil glava: je nova glava nodes[0]
    // ce je bil rep: je nov rep nodes[len-1]
    // ce je bil vmesen, povezi predhodnika (i-1) in naslednjika (i)
    if idx >= 0 && idx < len(nodes) {
        // naslednik je sedaj na idx (ker smo odstranili eno vozlisce)
        if idx > 0 && idx < len(nodes) { // imamo predhodnika in naslednika
            pred := nodes[idx-1]
            succ := nodes[idx]
            log.Printf("[REPAIR] reconnect pred=%s succ=%s", pred.NodeId, succ.NodeId)
            // initiate resync: pred -> succ missing writes
            go s.syncRangeBetween(pred, succ)
        } else if idx == 0 {
            // glava odstranjena: samo obvestimo; clients should re-resolve subscription node via control plane
            log.Printf("[REPAIR] head removed; new head=%s", nodes[0].NodeId)
        } else if idx == len(nodes) {
            // rep odstranjen, samo obvestimo; new tail is nodes[len-1]
            log.Printf("[REPAIR] tail removed; new tail=%s", nodes[len(nodes)-1].NodeId)
        }
    }
}


// buildConfigForIndex prebere stanje verige, uposteva meje in izracuna predhodnika in naslednika in vrne ChainConfig
func buildConfigForIndex(idx int, state *ChainState) *pb.ChainConfig {
    nodes, version := state.NodesSnapshot()
    
    //ce je veriga še prazna
    if len(nodes) == 0 {
        return &pb.ChainConfig{ChainVersion: version}
    }

    var pred, succ *pb.NodeInfo

    if idx > 0 {
        pred = nodes[idx-1]
    }
    if idx < len(nodes)-1 {
        succ = nodes[idx+1]
    }

    return &pb.ChainConfig{
        ChainVersion:     version,
        Head:        nodes[0],
        Tail:        nodes[len(nodes)-1],
        Predecessor: pred,
        Successor:   succ,
        Nodes: nodes,
    }
}

// notifyAllNodes se klice vsakic ko controlserver dobi nov join v verigo, 
// zato da vsem starejsim vozliscem posodobimo ChainConfig na pod. ravnini
func (s *ControlServer) notifyAllNodes() {
    nodes, _ := s.state.NodesSnapshot()

    for idx, node := range nodes {
        go func(i int, n *pb.NodeInfo) {
            // dial with timeout and WithBlock, retry once
            ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
            defer cancel()

            conn, err := grpc.DialContext(ctx, n.Address, grpc.WithInsecure(), grpc.WithBlock())
            if err != nil {
                log.Printf("[NOTIFY] dial failed %s (%s): %v", n.NodeId, n.Address, err)

                // one quick retry
                time.Sleep(200 * time.Millisecond)
                ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
                defer cancel2()
                conn, err = grpc.DialContext(ctx2, n.Address, grpc.WithInsecure(), grpc.WithBlock())
                if err != nil {
                    log.Printf("[NOTIFY] retry failed %s (%s): %v", n.NodeId, n.Address, err)
                    return
                }
            }
            defer conn.Close()

            client := pb.NewDataNodeClient(conn)

            // Build config specific for this node index
            cfg := buildConfigForIndex(i, s.state)

            // send UpdateChainConfig
            ctxRPC, cancelRPC := context.WithTimeout(context.Background(), 5*time.Second)
            defer cancelRPC()
            if _, err := client.UpdateChainConfig(ctxRPC, cfg); err != nil {
                log.Printf("[NOTIFY] UpdateChainConfig failed for %s: %v", n.NodeId, err)
                return
            }

            log.Printf("[NOTIFY] UpdateChainConfig sent to node=%s addr=%s chainver=%d", n.NodeId, n.Address, cfg.ChainVersion)
        }(idx, node)
    }
}

// Join klicejo vozlisca iz podatkovne ravnine ko se zelijo povezati
func (s *ControlServer) Join(ctx context.Context, node *pb.NodeInfo) (*pb.ChainConfig, error) {
    log.Printf("[JOIN] node_id=%s addr=%s\n", node.NodeId, node.Address)
    idx := s.state.AddNode(node)
    newConfig := buildConfigForIndex(idx, s.state)
    nodes, _ := s.state.NodesSnapshot()
    if idx == len(nodes)-1 && idx > 0 {
        // new tail: sync from old tail (nodes[idx-1])
        oldTail := nodes[idx-1]
        go func() {
            s.syncRangeBetween(oldTail, node)
            // now that sync done, push configs
            s.notifyAllNodes()
        }()
        // return initially constructed cfg to the joining node
        return newConfig, nil
    }
    s.notifyAllNodes()
    return newConfig, nil
}

// GetClusterState vrne stanje verige (no predecessor/successor)
func (s *ControlServer) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*pb.ChainConfig, error) {
    head, tail, version := s.state.Snapshot()

    return &pb.ChainConfig{
        ChainVersion: version,
        Head:  head,
        Tail:  tail,
    }, nil
}
