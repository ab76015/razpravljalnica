package main

import (
    "flag"
    "log"
    "fmt"
    "net"
    "time"
    "context"
    "google.golang.org/grpc"
   "google.golang.org/protobuf/proto"
    pb "github.com/ab76015/razpravljalnica/api/pb"
    "github.com/ab76015/razpravljalnica/internal/storage"
    "github.com/ab76015/razpravljalnica/internal/replication"
    server "github.com/ab76015/razpravljalnica/internal/server"
)

var (
    serverHost      = flag.String("host", "localhost", "Server host")
    serverPort      = flag.String("port", "50051", "Server port")
    controlAddress  = flag.String("control", "localhost:60051", "Control plane address")
    nodeID          = flag.String("node_id", "", "Unique node ID")
)


func main() {
    flag.Parse()
    listenAddr := fmt.Sprintf("%s:%s", *serverHost, *serverPort)
    lis, err := net.Listen("tcp", listenAddr) //port kjer prejemamo client requeste
    if err != nil {
        log.Fatalf("Failed to listen on %s: %v", listenAddr, err)
    }

    grpcServer := grpc.NewServer() //ustvari instanco grpc serverja

    storage := storage.NewMemStorage()

    if *nodeID == "" {
        log.Fatal("node_id must be provided")
    }

    self := &pb.NodeInfo{
        NodeId:  *nodeID,
        Address: listenAddr,
    }
    
    nodeState := replication.NewNodeState(self)
    
    replicationSrv := replication.NewDataNodeServer(nodeState, storage)
    messageBoardSrv := server.NewMessageBoardServer(storage, replicationSrv)
    
    // povezemo s kontrolno ravnino
    conn, err := grpc.Dial(*controlAddress, grpc.WithInsecure())
    if err != nil {
        log.Fatalf("failed to dial control plane: %v", err)
    }
    defer conn.Close()
    client := pb.NewControlPlaneClient(conn)

    cfg, err := client.Join(context.Background(), self)
    if err != nil {
        log.Fatalf("join failed: %v", err)
    }
    nodeState.UpdateConfig(cfg)

    // povezemo z nasimi implementacijami
    pb.RegisterMessageBoardServer(grpcServer, messageBoardSrv)
    pb.RegisterDataNodeServer(grpcServer, replicationSrv)


    log.Printf("Data plane server listening on %s\n", listenAddr)
    if err := grpcServer.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}

