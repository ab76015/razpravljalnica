package main

import (
    "flag"
    "log"
    "fmt"
    "net"
    "time"
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
    
    // povezemo z nasimi implementacijami
    pb.RegisterMessageBoardServer(grpcServer, messageBoardSrv)
    pb.RegisterDataNodeServer(grpcServer, replicationSrv)

    // poklicemo join() na control server da registriramo node na kontrolni ravnini
    // testiranje pisanja v verigi (ONLY FOR TESTING)
    go func() {
        time.Sleep(5 * time.Second) // give chain time to stabilize

        if !nodeState.IsHead() {
            return
        }

        log.Println("[TEST] initiating manual write from head")

        // 1. pripravimo pravi protobuf request
        postReq := &pb.PostMessageRequest{
            TopicId:  1,
            UserId:   1,
            Text:     "hello chain replication",
        }

        payload, err := proto.Marshal(postReq)
        if err != nil {
            log.Printf("[TEST] marshal failed: %v", err)
            return
        }

        // 2. nova verzija zapisa
        version := nodeState.NextVersion()

        rw := &pb.ReplicatedWrite{
            Version: version,
            Op:      "PostMessage",
            Payload: payload,
        }

        // 3. registriramo ACK čakanje
        ackCh := replicationSrv.RegisterPendingACK(version)
        defer replicationSrv.CancelPendingACK(version)

        // 4. lokalni apply (head mora tudi pisati!)
        if err := replicationSrv.ApplyWrite(rw); err != nil {
            log.Printf("[TEST] local apply failed: %v", err)
            return
        }

        // 5. pošlji nasledniku
        if err := replicationSrv.ForwardWrite(rw); err != nil {
            log.Printf("[TEST] forward failed: %v", err)
            return
        }

        // 6. čakaj na ACK iz repa
        select {
        case <-ackCh:
            log.Printf("[TEST] write version=%d committed", version)
        case <-time.After(5 * time.Second):
            log.Printf("[TEST] ACK timeout for version=%d", version)
        }
    }()


    log.Printf("Data plane server listening on %s\n", listenAddr)
    if err := grpcServer.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}

