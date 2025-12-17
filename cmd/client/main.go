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

    u, _ := client.CreateUser(ctx, &pb.CreateUserRequest{Name: "user1"})
    t, _ := client.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "TestTopic"})
    t2, _ := client.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "TestTopicTwo"})
    m, _ := client.PostMessage(ctx, &pb.PostMessageRequest{
        TopicId: t.Id,
        UserId:  u.Id,
        Text:    "hello world",
    })
    m2, _ := client.PostMessage(ctx, &pb.PostMessageRequest{
        TopicId: t.Id,
        UserId:  u.Id,
        Text:    "hello world two",
    })
    m3, _ := client.PostMessage(ctx, &pb.PostMessageRequest{
        TopicId: t2.Id,
        UserId:  u.Id,
        Text:    "hello world topic 2",
    })
    fmt.Println(u)
    fmt.Println(t)
    fmt.Println(t2)
    fmt.Println(m , "\n", m2, "\n", m3)
    m, _ = client.UpdateMessage(ctx, &pb.UpdateMessageRequest{
        TopicId: m.TopicId,
        UserId:  m.UserId,
        MessageId: m.Id,
        Text:    "hello another world",
    })
    m, _ = client.LikeMessage(ctx, &pb.LikeMessageRequest{
        TopicId: t.Id,
        UserId:  u.Id,
        MessageId: m.Id,
    })
    fmt.Println(m)

    topics, _ := client.ListTopics(ctx, &emptypb.Empty{})

    for _, topic := range topics.Topics {
        fmt.Println("topics", topic)
    }

    fmt.Println("topic 1 messages:")
    messages, _ := client.GetMessages(ctx, &pb.GetMessagesRequest{
        TopicId: t.Id,
        FromMessageId: 0,
        Limit: 5,
    })

    for _, m := range messages.Messages {
        fmt.Println(m)
    }

    _, _ = client.DeleteMessage(ctx, &pb.DeleteMessageRequest{
        TopicId: m.TopicId,
        UserId:  m.UserId,
        MessageId: m.Id,
    })

    fmt.Println("topic 1 messages post delete:")
    messages, _ = client.GetMessages(ctx, &pb.GetMessagesRequest{
        TopicId: t.Id,
        FromMessageId: 0,
        Limit: 5,
    })

    for _, m := range messages.Messages {
        fmt.Println(m)
    }

    m, _ = client.LikeMessage(ctx, &pb.LikeMessageRequest{
        TopicId: t.Id,
        UserId:  u.Id,
        MessageId: m.Id,
    })
    m, _ = client.LikeMessage(ctx, &pb.LikeMessageRequest{
        TopicId: t.Id,
        UserId:  u.Id,
        MessageId: m.Id,
    })
    fmt.Println(m)
}   
