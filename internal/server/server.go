package server

import (
	"context"
	"fmt"

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

// Pravilo: Za pisalne metode se najprej replicira nato zapiše v storage! (Replication pipeline aplicira write tj. replication/node.go)

// CreateUser je pisalna metoda, ki ustvari novega uporabnika
func (s *Server) CreateUser(ctx context.Context, in *pb.CreateUserRequest) (*pb.User, error) {
	// v ns shrani nodestate oz. stanje ki ga dobiš preko getterja State(),
	// ta je definiran v replication/node.go za DataNodeServer
	ns := s.replication.State()
	if !ns.IsHead() {
		fmt.Println("server.go: CreateUser() error: writes are only allowed on head node!")
		return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
	}

	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}

	// Zgradi sporočilo za replikacijo (glej proto replicatedwrite)
	rw := &pb.ReplicatedWrite{
		Version: ns.Version(),
		Op:      "CreateUser",
		Payload: data,
	}

	// Repliciraj/pošlji nasledniku (v verigi)
	if err := s.replication.ForwardWrite(rw); err != nil {
		return nil, err
	}

	// user, err := s.storage.CreateTopic(in.Name)
	// if err != nil {
	// 	return nil, err
	// }

	//zaenkrat vrnemo optimistečen odgovor, ampak treba implementira wait za ack
	// return &pb.User{Id: user.ID, Name: user.Name}, nil
	return &pb.User{}, nil
}

// CreateTopic je pisalna metoda, ki ustvari novo temo
func (s *Server) CreateTopic(ctx context.Context, in *pb.CreateTopicRequest) (*pb.Topic, error) {
	ns := s.replication.State()
	if !ns.IsHead() {
		fmt.Println("server.go: CreateTopic() error: writes are only allowed on head node!")
		return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
	}

	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}

	// Zgradi sporočilo za replikacijo (glej proto replicatedwrite)
	rw := &pb.ReplicatedWrite{
		Version: ns.Version(),
		Op:      "CreateTopic",
		Payload: data,
	}

	// Repliciraj/pošlji nasledniku (v verigi)
	if err := s.replication.ForwardWrite(rw); err != nil {
		return nil, err
	}

	// topic, err := s.storage.CreateTopic(in.Name)
	// if err != nil {
	// 	return nil, err
	// }

	// return &pb.Topic{Id: topic.ID, Name: topic.Name}, nil
	//zaenkrat vrnemo optimistečen odgovor, ampak treba implementira wait za ack
	return &pb.Topic{}, nil
}

// PostMessage je pisalna metoda, ki ustvari novo sporočilo
func (s *Server) PostMessage(ctx context.Context, in *pb.PostMessageRequest) (*pb.Message, error) {
	ns := s.replication.State()
	if !ns.IsHead() {
		fmt.Println("server.go: PostMessage() error: writes are only allowed on head node!")
		return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
	}

	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}

	version := ns.NextVersion()
	// Zgradi sporočilo za replikacijo (glej proto replicatedwrite)
	rw := &pb.ReplicatedWrite{
		Version: version,
		Op:      "PostMessage",
		Payload: data,
	}

	// Registriraj čakajoči ACK
	chanACK := s.replication.RegisterPendingACK(version)

	if err := s.replication.ApplyWrite(rw); err != nil {
		return nil, err
	}

	// Repliciraj/pošlji nasledniku (v verigi)
	if err := s.replication.ForwardWrite(rw); err != nil {
		return nil, err
	}

	select {
	case <-chanACK:
		// ACK prejet
	case <-ctx.Done():
		s.replication.CancelPendingACK(version)
		return nil, status.Errorf(codes.DeadlineExceeded, "PostMessage not acknowledged in time")
	}

	// message, err := s.storage.PostMessage(in.TopicId, in.UserId, in.Text)
	// if err != nil {
	// 	return nil, err
	// }

	//return &pb.Message{Id: message.ID, TopicId: message.TopicID, UserId: message.UserID, Text: message.Text, CreatedAt: timestamppb.New(message.CreatedAt), Likes: message.Likes}, nil
	//zaenkrat vrnemo optimistečen odgovor, ampak treba implementira wait za ack
	return &pb.Message{}, nil
}

// UpdateMessage je pisalna metoda, ki posodobi obstoječe sporočilo
func (s *Server) UpdateMessage(ctx context.Context, in *pb.UpdateMessageRequest) (*pb.Message, error) {
	ns := s.replication.State()
	if !ns.IsHead() {
		fmt.Println("server.go: UpdateMessage() error: writes are only allowed on head node!")
		return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
	}

	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}

	// Zgradi sporočilo za replikacijo (glej proto replicatedwrite)
	rw := &pb.ReplicatedWrite{
		Version: ns.Version(),
		Op:      "UpdateMessage",
		Payload: data,
	}

	// Repliciraj/pošlji nasledniku (v verigi)
	if err := s.replication.ForwardWrite(rw); err != nil {
		return nil, err
	}

	// message, err := s.storage.UpdateMessage(in.TopicId, in.UserId, in.MessageId, in.Text)
	// if err != nil {
	// 	return nil, err
	// }

	//return &pb.Message{Id : message.ID, TopicId: message.TopicID, UserId: message.UserID, Text: message.Text, CreatedAt: timestamppb.New(message.CreatedAt), Likes: message.Likes}, nil
	//zaenkrat vrnemo optimistečen odgovor, ampak treba implementira wait za ack
	return &pb.Message{}, nil
}

// DeleteMessage je pisalna metoda, ki izbrise obstoječe sporočilo
func (s *Server) DeleteMessage(ctx context.Context, in *pb.DeleteMessageRequest) (*emptypb.Empty, error) {
	ns := s.replication.State()
	if !ns.IsHead() {
		fmt.Println("server.go: DeleteMessage() error: writes are only allowed on head node!")
		return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
	}
	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}

	// Zgradi sporočilo za replikacijo (glej proto replicatedwrite)
	rw := &pb.ReplicatedWrite{
		Version: ns.Version(),
		Op:      "DeleteMessage",
		Payload: data,
	}

	// Repliciraj/pošlji nasledniku (v verigi)
	if err := s.replication.ForwardWrite(rw); err != nil {
		return nil, err
	}

	// err = s.storage.DeleteMessage(in.TopicId, in.UserId, in.MessageId)
	// if err != nil {
	// 	return nil, err
	// }

	//zaenkrat vrnemo optimistečen odgovor, ampak treba implementira wait za ack
	return &emptypb.Empty{}, nil
}

// LikeMessage je pisalna metoda, ki vsečka sporočilo
func (s *Server) LikeMessage(ctx context.Context, in *pb.LikeMessageRequest) (*pb.Message, error) {
	ns := s.replication.State()
	if !ns.IsHead() {
		fmt.Println("server.go: LikeMessage() error: writes are only allowed on head node!")
		return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
	}

	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}

	// Zgradi sporočilo za replikacijo (glej proto replicatedwrite)
	rw := &pb.ReplicatedWrite{
		Version: ns.Version(),
		Op:      "LikeMessage",
		Payload: data,
	}

	// Repliciraj/pošlji nasledniku (v verigi)
	if err := s.replication.ForwardWrite(rw); err != nil {
		return nil, err
	}

	// message, err := s.storage.LikeMessage(in.TopicId, in.MessageId, in.UserId)
	// if err != nil {
	// 	return nil, err
	// }

	//return &pb.Message{Id : message.ID, TopicId: message.TopicID, UserId: message.UserID, Text: message.Text, CreatedAt: timestamppb.New(message.CreatedAt), Likes: message.Likes}, nil
	//zaenkrat vrnemo optimistečen odgovor, ampak treba implementira wait za ack
	return &pb.Message{}, nil
}

func (s *Server) ListTopics(ctx context.Context, _ *emptypb.Empty) (*pb.ListTopicsResponse, error) {
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

func (s *Server) GetMessages(ctx context.Context, in *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
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
