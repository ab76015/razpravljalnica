package main

import (
    "log"
    "net"

    "google.golang.org/grpc"
    pb "github.com/ab76015/razpravljalnica/api/pb"
    "github.com/ab76015/razpravljalnica/internal/storage"
)

func main() {
    /*
    lis, _ := net.Listen("tcp", ":50051")
    memStore := storage.NewMemStorage()
    srv := server.NewServer(memStore)

    grpcServer := grpc.NewServer()
    pb.RegisterMessageBoardServer(grpcServer, srv)

    grpcServer.Serve(lis)*/
}

