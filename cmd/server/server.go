package server

import (
    "context"
    pb "github.com/ab76015/razpravljalnica/api/pb"
    "github.com/ab76015/razpravljalnica/internal/storage"
)

type Server struct {
    pb.UnimplementedMessageBoardServer
    storage storage.Storage
}

func NewServer(s storage.Storage) *Server {
    return &Server{storage: s}
}

func (s *Server) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
    user, err := s.storage.CreateUser(req.Name)
    if err != nil {
        return nil, err
    }
    return &pb.User{Id: user.ID, Name: user.Name}, nil
}

