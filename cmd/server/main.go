package main

import (
    "log"
    "net"

    "google.golang.org/grpc"
    pb "github.com/ab76015/razpravljalnica/api/pb"
)

type server struct {
    pb.UnimplementedMessageBoardServer
}

func main() {
    lis, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatalf("listen error: %v", err)
    }

    s := grpc.NewServer()
    pb.RegisterMessageBoardServer(s, &server{})

    log.Println("Server listening on :50051")
    if err := s.Serve(lis); err != nil {
        log.Fatalf("server error: %v", err)
    }
}

