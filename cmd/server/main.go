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
    // Ustvarimo TCP server na vratih 50051
    lis, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatalf("listen error: %v", err)
    }
    // Ustvarimo gRPC server
    s := grpc.NewServer()
    // Registriramo implementacijo servisa (MessageBoard) pri gRPC strežniku s
    pb.RegisterMessageBoardServer(s, &server{})

    log.Println("Server listening on :50051")
    // Začni sprejemati dohodne omrežne povezave na lis (listenerju) in servirati gRPC klice
    if err := s.Serve(lis); err != nil {
        log.Fatalf("server error: %v", err)
    }
}

