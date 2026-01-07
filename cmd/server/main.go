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

    // poklicemo join() na control server da registriramo node na kontrolni ravnini
    // testiranje pisanja v verigi (ONLY FOR TESTING)
    /*
    go func() {
        time.Sleep(5 * time.Second) // allow chain to stabilize

        log.Printf("[TEST] am I head? %v", nodeState.IsHead())
        if !nodeState.IsHead() {
            return
        }

        log.Println("[TEST] initiating CreateTopic replication test")

        // 1. Build a write with NO preconditions
        req := &pb.CreateTopicRequest{
            Name: "chain-replication-test",
        }

        payload, err := proto.Marshal(req)
        if err != nil {
            log.Printf("[TEST] marshal failed: %v", err)
            return
        }

        // 2. Allocate version
        writeID := nodeState.NextWriteID()

        rw := &pb.ReplicatedWrite{
            WriteId: writeID,
            Op:      "CreateTopic",
            Payload: payload,
        }

        // 3. Register ACK wait
        ackCh := replicationSrv.RegisterPendingACK(writeID)
        defer replicationSrv.CancelPendingACK(writeID)

        // 4. Replicate from head (LOCAL APPLY + FORWARD)
        if err := replicationSrv.ReplicateFromHead(rw); err != nil {
            log.Printf("[TEST] replicate failed: %v", err)
            return
        }

        // 5. Wait for ACK from tail
        select {
        case <-ackCh:
            log.Printf("[TEST] SUCCESS: version=%d fully replicated & committed", writeID)
        case <-time.After(5 * time.Second):
            log.Printf("[TEST] FAILURE: ACK timeout for version=%d", writeID)
        }
    }()*/


    log.Printf("Data plane server listening on %s\n", listenAddr)
    if err := grpcServer.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}

