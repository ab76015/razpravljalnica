package control

import (
    "context"
    "log"
    "time"
    "google.golang.org/grpc"
    pb "github.com/ab76015/razpravljalnica/api/pb"
    "google.golang.org/protobuf/types/known/emptypb"
)

type ControlServer struct {
    pb.UnimplementedControlPlaneServer
    state *ChainState
}

func NewControlServer(state *ChainState) *ControlServer {
    return &ControlServer{state: state}
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
            conn, err := grpc.Dial(n.Address,grpc.WithInsecure(),)
            if err != nil {
                log.Printf("Failed to dial node %s at %s: %v\n", n.NodeId, n.Address, err)
                return
            }
            defer conn.Close()

            client := pb.NewDataNodeClient(conn)

            ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
            defer cancel()

            // Build config for THIS node's index
            cfg := buildConfigForIndex(i, s.state)

            _, err = client.UpdateChainConfig(ctx, cfg)
            if err != nil {
                log.Printf("Failed to update config for node %s: %v\n", n.NodeId, err)
            } else {
                log.Printf("Updated config sent to node %s with addr %s\n", n.NodeId, n.Address)
            }
        }(idx, node)
    }
}

// Join klicejo vozlisca iz podatkovne ravnine ko se zelijo povezati
func (s *ControlServer) Join(ctx context.Context, node *pb.NodeInfo) (*pb.ChainConfig, error) {
    log.Printf("[JOIN] node_id=%s addr=%s\n", node.NodeId, node.Address)
    idx := s.state.AddNode(node)
    newConfig := buildConfigForIndex(idx, s.state)

    // Push nov config vsem starim vozliscem v podatkovni ravnini asinhrono
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

