package main

import (
    "log"
    "net"

    "google.golang.org/grpc"

    pb "github.com/ab76015/razpravljalnica/api/pb"
    control "github.com/ab76015/razpravljalnica/internal/control"
)

func main() {
    // inicializacija stanja
    controlState := control.NewChainState()

    // kontrolni strežnik
    srv := control.NewControlServer(controlState)

    // START monitor (heartbeats)
    srv.StartMonitor()

    lis, err := net.Listen("tcp", ":60051")
    if err != nil {
        log.Fatalf("Control server failed to listen: %v", err)
    }

    grpcServer := grpc.NewServer()
    pb.RegisterControlPlaneServer(grpcServer, srv)

    log.Printf("Control server listening on :60051")

    if err := grpcServer.Serve(lis); err != nil {
        log.Fatalf("Control server failed to serve: %v", err)
    }
}

