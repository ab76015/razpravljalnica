package main

import (
    "flag"
    "log"
    "fmt"
    "net"
    "context"
    "time"
    "google.golang.org/grpc"
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
    messageBoardSrv := server.NewMessageBoardServer(storage)

    nodeState := replication.NewNodeState()
    replicationSrv := replication.NewDataNodeServer(nodeState)

    // povezemo z nasimi implementacijami
    pb.RegisterMessageBoardServer(grpcServer, messageBoardSrv)
    pb.RegisterDataNodeServer(grpcServer, replicationSrv)

    // poklicemo join() na control server da registriramo node na kontrolni ravnini
    go func() {
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()

        conn, err := grpc.Dial(*controlAddress, grpc.WithInsecure())
        if err != nil {
            log.Fatalf("Failed to connect to control server at %s: %v", *controlAddress, err)
        }
        defer conn.Close()

        controlClient := pb.NewControlPlaneClient(conn)

        if *nodeID == "" {
            log.Fatal("node_id flag is required")
        }

        nodeInfo := &pb.NodeInfo{
            NodeId:  *nodeID,
            Address: listenAddr,
        }

        config, err := controlClient.Join(ctx, nodeInfo)
        if err != nil {
            log.Fatalf("Join call failed: %v", err)
        }

        log.Printf("[JOINED] chain with chain-version: %d predecessor: (%v) successor: (%v)\n",
            config.Version, config.Predecessor, config.Successor)

        // Update local node state from config as needed, e.g.:
        nodeState.UpdateConfig(config)
    }()


    log.Printf("Data plane server listening on %s\n", listenAddr)
    if err := grpcServer.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}

