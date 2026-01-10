package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	pb "github.com/ab76015/razpravljalnica/api/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

var serverAddr = flag.String("addr", "localhost:50051", "The data node address (host:port)")

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
	subResp, err := client.GetSubcscriptionNode(context.Background(), subReq)
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

	// 5) Post a message (head path). This should cause a committed event to be emitted.
	postCtx, postCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer postCancel()

	_, err = client.PostMessage(postCtx, &pb.PostMessageRequest{
		TopicId: topicID,
		UserId:  userID,
		Text:    "Hello from client, testing subscription",
	})
	if err != nil {
		log.Fatalf("PostMessage failed: %v", err)
	}
	fmt.Println("Posted message, now waiting for committed event...")

	// 6) Wait for a single event or timeout
	select {
	case ev := <-eventCh:
		fmt.Printf("Received event: seq=%d op=%v message_id=%d topic=%d text=%q\n",
			ev.SequenceNumber, ev.Op, ev.Message.Id, ev.Message.TopicId, ev.Message.Text)
	case err := <-errCh:
		log.Fatalf("stream recv ended: %v", err)
	case <-time.After(6 * time.Second):
		log.Fatalf("timeout waiting for event")
	}
}
   
