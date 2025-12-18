package main

import (
    "flag"
    "context"
    "fmt"
    "google.golang.org/grpc" 
    pb "github.com/ab76015/razpravljalnica/api/pb"
    "google.golang.org/protobuf/types/known/emptypb"
)

var serverAddr = flag.String("addr", "localhost:50051", "The server address in the format of host:port")

func main() {
    flag.Parse()
    // Da klicemo service metode moramo prvo ustvariti gRPC kanal za komunikacijo s streznikom.
    // To ustvarimo tako da podamo naslov serverja (host:port) v grpc.Dial()
    conn, err := grpc.Dial(*serverAddr, grpc.WithInsecure())
    if err != nil {
        panic(err)     
    }
    defer conn.Close()
    //ko imamo gRPC kanal, potrebujemo client stub za RPC; dobimo ga z NewMessageBoardClient iz pb package
    client := pb.NewMessageBoardClient(conn)
    
    ctx := context.Background()
}   
