package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	pb "github.com/ab76015/razpravljalnica/api/pb"
	"google.golang.org/grpc"
)

var serverAddr = flag.String("addr", "localhost:60051", "The data node address (host:port)")

func main() {
	flag.Parse()

	conn, err := grpc.Dial(*serverAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial server %s: %v", *serverAddr, err)
	}
	defer conn.Close()

	client := pb.NewMessageBoardClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1) Create user
	uResp, err := client.CreateUser(ctx, &pb.CreateUserRequest{Name: "tester"})
	if err != nil {
		log.Fatalf("CreateUser failed: %v", err)
	}
	userID := uResp.Id
	fmt.Printf("Created user id=%d\n", userID)

	// 2) Create topic
	tResp, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "test-topic"})
	if err != nil {
		log.Fatalf("CreateTopic failed: %v", err)
	}
	topicID := tResp.Id
	fmt.Printf("Created topic id=%d\n", topicID)

	// 3) Ask node for a subscription node + token (head issues this)
	subReq := &pb.SubscriptionNodeRequest{
		UserId:  userID,
		TopicId: []int64{topicID},
	}
	subResp, err := client.GetSubscriptionNode(context.Background(), subReq)
	if err != nil {
		log.Fatalf("GetSubscriptionNode failed: %v", err)
	}
	fmt.Printf("Got subscription node: %+v tokenLen=%d\n", subResp.Node, len(subResp.SubscribeToken))

	// 4) Dial the returned node and open subscribe stream
	subConn, err := grpc.Dial(subResp.Node.Address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("dial to subscribe node failed: %v", err)
	}
	defer subConn.Close()
	subClient := pb.NewMessageBoardClient(subConn)

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	stream, err := subClient.SubscribeTopic(streamCtx, &pb.SubscribeTopicRequest{
		TopicId:        []int64{topicID},
		UserId:         userID,
		FromMessageId:  0,
		SubscribeToken: subResp.SubscribeToken,
	})
	if err != nil {
		log.Fatalf("SubscribeTopic RPC failed: %v", err)
	}

	// start goroutine to read stream
	eventCh := make(chan *pb.MessageEvent, 1)
	errCh := make(chan error, 1)
	go func() {
		for {
			ev, err := stream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			eventCh <- ev
		}
	}()
    // 5) Post a message
    postCtx, postCancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer postCancel()

    postResp, err := client.PostMessage(postCtx, &pb.PostMessageRequest{
        TopicId: topicID,
        UserId:  userID,
        Text:    "Hello from client, testing subscription",
    })
    if err != nil {
        log.Fatalf("PostMessage failed: %v", err)
    }
    msgID := postResp.MessageId
    fmt.Printf("Posted message id=%d, waiting for committed event...\n", msgID)

    // expect OP_POST committed event here


    // 6) Update the message
    updateCtx, updateCancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer updateCancel()

    _, err = client.UpdateMessage(updateCtx, &pb.UpdateMessageRequest{
        TopicId:   topicID,
        UserId:    userID,
        MessageId: msgID,
        Text:      "Edited message text",
    })
    if err != nil {
        log.Fatalf("UpdateMessage failed: %v", err)
    }
    fmt.Println("Updated message, waiting for committed update event...")

    // expect OP_UPDATE committed event here


    // 7) Like the message
    likeCtx, likeCancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer likeCancel()

    _, err = client.LikeMessage(likeCtx, &pb.LikeMessageRequest{
        TopicId:   topicID,
        UserId:    userID,
        MessageId: msgID,
    })
    if err != nil {
        log.Fatalf("LikeMessage failed: %v", err)
    }
    fmt.Println("Liked message, waiting for committed like event...")

    // expect OP_LIKE committed event here


    // 8) Delete the message
    deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer deleteCancel()

    _, err = client.DeleteMessage(deleteCtx, &pb.DeleteMessageRequest{
        TopicId:   topicID,
        UserId:    userID,
        MessageId: msgID,
    })
    if err != nil {
        log.Fatalf("DeleteMessage failed: %v", err)
    }
    fmt.Println("Deleted message, waiting for committed delete event...")

    // expect OP_DELETE committed event here

}
   
