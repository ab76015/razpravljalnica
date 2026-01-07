package server

import (
	"context"
	pb "github.com/ab76015/razpravljalnica/api/pb"
	"github.com/ab76015/razpravljalnica/internal/replication"
	"github.com/ab76015/razpravljalnica/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	pb.UnimplementedMessageBoardServer
	storage     storage.Storage
	replication *replication.DataNodeServer
}

func NewMessageBoardServer(s storage.Storage, r *replication.DataNodeServer) *Server {
	return &Server{storage: s, replication: r}
}

// CreateUser je pisalna metoda, ki ustvari novega uporabnika
func (s *Server) CreateUser(ctx context.Context, in *pb.CreateUserRequest) (*pb.User, error) {
	// v ns shrani nodestate oz. stanje ki ga dobiš preko getterja State(),
	// ta je definiran v replication/node.go za DataNodeServer
	ns := s.replication.State()
	if !ns.IsHead() {
		return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
	}

	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}

	// Zgradi sporočilo za replikacijo (glej proto replicatedwrite)
    writeID := ns.NextWriteID()
    rw := &pb.ReplicatedWrite{
		WriteId: writeID,
		Op:      "CreateUser",
		Payload: data,
	}

	// Registriraj čakajoči ACK
	chanACK := s.replication.RegisterPendingACK(writeID)
	
    // Repliciraj/pošlji nasledniku (v verigi)
    if err := s.replication.ReplicateFromHead(rw); err != nil {
        s.replication.CancelPendingACK(writeID)
        return nil, err
    }
	
    select {
        case <-chanACK:
            // ACK prejet
            return &pb.User{}, nil
        case <-ctx.Done():
            s.replication.CancelPendingACK(writeID)
            return nil, status.Errorf(codes.DeadlineExceeded, "PostMessage not acknowledged in time")
	}
}

// CreateTopic je pisalna metoda, ki ustvari novo temo
func (s *Server) CreateTopic(ctx context.Context, in *pb.CreateTopicRequest) (*pb.Topic, error) {
	ns := s.replication.State()
	if !ns.IsHead() {
		return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
	}

	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}

	// Zgradi sporočilo za replikacijo (glej proto replicatedwrite)
    writeID := ns.NextWriteID()
    rw := &pb.ReplicatedWrite{
		WriteId: writeID,
		Op:      "CreateTopic",
		Payload: data,
	}
	
	// Registriraj čakajoči ACK
	chanACK := s.replication.RegisterPendingACK(writeID)


	// Repliciraj/pošlji nasledniku (v verigi)
    if err := s.replication.ReplicateFromHead(rw); err != nil {
        s.replication.CancelPendingACK(writeID)
        return nil, err
    }

	select {
        case <-chanACK:
            // ACK prejet
            return &pb.Topic{}, nil
        case <-ctx.Done():
            s.replication.CancelPendingACK(writeID)
            return nil, status.Errorf(codes.DeadlineExceeded, "PostMessage not acknowledged in time")
	}
}

// PostMessage je pisalna metoda, ki ustvari novo sporočilo
func (s *Server) PostMessage(ctx context.Context, in *pb.PostMessageRequest) (*pb.Message, error) {
	ns := s.replication.State()
	if !ns.IsHead() {
		return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
	}

	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}

	// Zgradi sporočilo za replikacijo (glej proto replicatedwrite)
    writeID := ns.NextWriteID()
    rw := &pb.ReplicatedWrite{
		WriteId: writeID,
		Op:      "PostMessage",
		Payload: data,
	}

	// Registriraj čakajoči ACK
	chanACK := s.replication.RegisterPendingACK(writeID)

	// (Kot glava) Lokalno apliciraj in pošlji nasledniku (v verigi)
    if err := s.replication.ReplicateFromHead(rw); err != nil {
        s.replication.CancelPendingACK(writeID)
        return nil, err
    }

	select {
        case <-chanACK:
            // ACK prejet
            return &pb.Message{}, nil
        case <-ctx.Done():
            s.replication.CancelPendingACK(writeID)
            return nil, status.Errorf(codes.DeadlineExceeded, "PostMessage not acknowledged in time")
	}
}

// UpdateMessage je pisalna metoda, ki posodobi obstoječe sporočilo
func (s *Server) UpdateMessage(ctx context.Context, in *pb.UpdateMessageRequest) (*pb.Message, error) {
	ns := s.replication.State()
	if !ns.IsHead() {
		return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
	}

	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}

	// Zgradi sporočilo za replikacijo (glej proto replicatedwrite)
    writeID := ns.NextWriteID()
    rw := &pb.ReplicatedWrite{
		WriteId: writeID,
		Op:      "UpdateMessage",
		Payload: data,
	}

	// Registriraj čakajoči ACK
	chanACK := s.replication.RegisterPendingACK(writeID)

	// (Kot glava) Lokalno apliciraj in pošlji nasledniku (v verigi)
    if err := s.replication.ReplicateFromHead(rw); err != nil {
        s.replication.CancelPendingACK(writeID)
        return nil, err
    }

	select {
        case <-chanACK:
            return &pb.Message{}, nil
        case <-ctx.Done():
            s.replication.CancelPendingACK(writeID)
            return nil, status.Errorf(codes.DeadlineExceeded, "PostMessage not acknowledged in time")
	}
}

// DeleteMessage je pisalna metoda, ki izbrise obstoječe sporočilo
func (s *Server) DeleteMessage(ctx context.Context, in *pb.DeleteMessageRequest) (*emptypb.Empty, error) {
	ns := s.replication.State()
	if !ns.IsHead() {
		return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
	}
	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}

	// Zgradi sporočilo za replikacijo (glej proto replicatedwrite)
    writeID := ns.NextWriteID()
    rw := &pb.ReplicatedWrite{
		WriteId: writeID,
		Op:      "DeleteMessage",
		Payload: data,
	}
    
    // Registriraj čakajoči ACK
	chanACK := s.replication.RegisterPendingACK(writeID)
    
    if err := s.replication.ReplicateFromHead(rw); err != nil {
        s.replication.CancelPendingACK(writeID)
        return nil, err 
    }   

    select {
        case <-chanACK:
            return &emptypb.Empty{}, nil 
        case <-ctx.Done():
            s.replication.CancelPendingACK(writeID)
            return nil, status.Errorf(codes.DeadlineExceeded, "PostMessage not acknowledged in time")
    }
}

// LikeMessage je pisalna metoda, ki vsečka sporočilo
func (s *Server) LikeMessage(ctx context.Context, in *pb.LikeMessageRequest) (*pb.Message, error) {
	ns := s.replication.State()
	if !ns.IsHead() {
		return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
	}

	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}

	// Zgradi sporočilo za replikacijo (glej proto replicatedwrite)
    writeID := ns.NextWriteID()

    rw := &pb.ReplicatedWrite{
		WriteId: writeID,
		Op:      "LikeMessage",
		Payload: data,
	}

	// Registriraj čakajoči ACK
	chanACK := s.replication.RegisterPendingACK(writeID)

	// Repliciraj/pošlji nasledniku (v verigi)
    if err := s.replication.ReplicateFromHead(rw); err != nil {
        s.replication.CancelPendingACK(writeID)
        return nil, err 
    }   

    select {
        case <-chanACK:
            return &pb.Message{}, nil 
        case <-ctx.Done():
            s.replication.CancelPendingACK(writeID)
            return nil, status.Errorf(codes.DeadlineExceeded, "PostMessage not acknowledged in time")
    }
}

// requireTail preveri, če je vozlišče rep
func requireTail(ns *replication.NodeState) error {
	if !ns.IsTail() {
		return status.Errorf(
			codes.FailedPrecondition,
			"reads are allowed only on tail node",
		)
	}
	return nil
}


// ListTopics je enkratna bralna metoda (se bere iz repa).
func (s *Server) ListTopics(ctx context.Context, _ *emptypb.Empty) (*pb.ListTopicsResponse, error) {
	ns := s.replication.State()
	if err := requireTail(ns); err != nil {
		return nil, err
	}
    topics, err := s.storage.ListTopics()
	if err != nil {
		return nil, err
	}
	response := &pb.ListTopicsResponse{}
	for _, topic := range topics {
		response.Topics = append(response.Topics, &pb.Topic{Id: topic.ID, Name: topic.Name})
	}
	return response, nil
}

// GetMessages je enkratna bralna metoda (se bere iz repa).
func (s *Server) GetMessages(ctx context.Context, in *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	ns := s.replication.State()
	if err := requireTail(ns); err != nil {
		return nil, err
	}
    msgs, err := s.storage.GetMessages(in.TopicId, in.FromMessageId, in.Limit)
	if err != nil {
		return nil, err
	}

	response := &pb.GetMessagesResponse{}
	for _, m := range msgs {
		response.Messages = append(response.Messages, &pb.Message{Id: m.ID, TopicId: m.TopicID, UserId: m.UserID, Text: m.Text, CreatedAt: timestamppb.New(m.CreatedAt), Likes: m.Likes})
	}
	return response, nil
}
