package main

import (
    "log"
    "net"

    "google.golang.org/grpc"
    pb "github.com/ab76015/razpravljalnica/api/pb"
    "github.com/ab76015/razpravljalnica/internal/storage"
)

func main() {
    lis, err := net.Listen("tcp", ":50051") //port kjer prejemamo client requeste
    if err != nil {
        log.Fatal(err)
    }

    grpcServer := grpc.NewServer() //ustvari instanco grpc serverja

    st := storage.NewMemStorage()
    srv := NewServer(st)

    pb.RegisterMessageBoardServer(grpcServer, srv) //povezemo z naso implementacijo

    log.Println("listening on :50051")
    grpcServer.Serve(lis) //server tu blokira do klica Stop() ali dokler proces ni killed
}

