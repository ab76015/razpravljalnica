package control

import (
    "context"
    pb "github.com/ab76015/razpravljalnica/api/pb"    
)

type ControlServer struct {
    state *ChainState
    UnimplementedControlPlaneServer
}

func NewControlServer(state *ChainState) * ControlServer {
    return &ControlServer{state: state}
}

func (s *ControlServer) GetClusterState(ctx context.Context, _ *pb.Empty) (*pb.GetClusterStateResponse, error) {
    head, tail, _ := s.state.GetClusterState()

    return &pb.GetClusterStateResponse{
        Head: head,
        Tail: tail,
    }, nil
}
