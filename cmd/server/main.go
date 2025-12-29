package main

import (
    "log"
    "net"

    "google.golang.org/grpc"
    pb "github.com/ab76015/razpravljalnica/api/pb"
    "github.com/ab76015/razpravljalnica/internal/storage"
    "github.com/ab76015/razpravljalnica/internal/replication"
)

func main() {
    lis, err := net.Listen("tcp", ":50051") //port kjer prejemamo client requeste
    if err != nil {
        log.Fatal(err)
    }

    grpcServer := grpc.NewServer() //ustvari instanco grpc serverja

    storage := storage.NewMemStorage()
    messageBoardSrv := NewMessageBoardServer(storage)

    nodeState := replication.NewNodeState()
    replicationSrv := replication.NewDataNodeServer(nodeState)
    
    // povezemo z nasimi implementacijami
    pb.RegisterMessageBoardServer(grpcServer, messageBoardSrv)
    pb.RegisterDataNodeServer(grpcServer, replicationSrv)
    
    log.Println("listening on :50051")
    grpcServer.Serve(lis) //server tu blokira do klica Stop() ali dokler proces ni killed
}

