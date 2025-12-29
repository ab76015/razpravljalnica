package control

import (
    "context"

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
    nodes, epoch := state.NodesSnapshot()
    
    //ce je veriga še prazna
    if len(nodes) == 0 {
        return &pb.ChainConfig{Epoch: epoch}
    }

    var pred, succ *pb.NodeInfo

    if idx > 0 {
        pred = nodes[idx-1]
    }
    if idx < len(nodes)-1 {
        succ = nodes[idx+1]
    }

    return &pb.ChainConfig{
        Epoch:       epoch,
        Head:        nodes[0],
        Tail:        nodes[len(nodes)-1],
        Predecessor: pred,
        Successor:   succ,
    }
}


// Join klicejo vozlisca iz podatkovne ravnine ko se zelijo povezati
func (s *ControlServer) Join(ctx context.Context, node *pb.NodeInfo) (*pb.ChainConfig, error) {
    idx := s.state.AddNode(node)
    return buildConfigForIndex(idx, s.state), nil
}

// GetClusterState vrne stanje verige (no predecessor/successor)
func (s *ControlServer) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*pb.ChainConfig, error) {
    head, tail, epoch := s.state.Snapshot()

    return &pb.ChainConfig{
        Epoch: epoch,
        Head:  head,
        Tail:  tail,
    }, nil
}

