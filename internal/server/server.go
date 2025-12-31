package server

import (
    "fmt"
    "context"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
    "google.golang.org/protobuf/types/known/emptypb"
    "google.golang.org/protobuf/types/known/timestamppb"
    pb "github.com/ab76015/razpravljalnica/api/pb"
    "github.com/ab76015/razpravljalnica/internal/storage"
    "github.com/ab76015/razpravljalnica/internal/replication"
)

type Server struct {
    pb.UnimplementedMessageBoardServer
    storage storage.Storage
    nodeState *replication.NodeState
}

func NewMessageBoardServer(s storage.Storage, ns *replication.NodeState) *Server {
    return &Server{storage: s, nodeState: ns,}
}

// CreateUser je pisalna metoda, ki ustvari novega uporabnika
func (s *Server) CreateUser(ctx context.Context, in *pb.CreateUserRequest) (*pb.User, error) {
    if !s.nodeState.IsHead() {
        fmt.Println("server.go: CreateUser() error: writes are only allowed on head node!")
        return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
    }
    user, err := s.storage.CreateUser(in.Name)
    if err != nil {
        return nil, err
    }

    // TODO: forward to successor
    return &pb.User{Id: user.ID, Name: user.Name}, nil
}

// CreateTopic je pisalna metoda, ki ustvari novo temo
func (s *Server) CreateTopic(ctx context.Context, in *pb.CreateTopicRequest) (*pb.Topic, error) {
    if !s.nodeState.IsHead() {
        fmt.Println("server.go: CreateTopic() error: writes are only allowed on head node!")
        return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
    } 
    topic, err := s.storage.CreateTopic(in.Name)
    if err != nil {
        return nil, err
    }
    // TODO: forward to successor
    return &pb.Topic{Id: topic.ID, Name: topic.Name }, nil
}

// PostMessage je pisalna metoda, ki ustvari novo sporočilo
func (s *Server) PostMessage(ctx context.Context, in *pb.PostMessageRequest) (*pb.Message, error) {
    if !s.nodeState.IsHead() {
        fmt.Println("server.go: PostMessage() error: writes are only allowed on head node!")
        return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
    } 
    message, err := s.storage.PostMessage(in.TopicId, in.UserId, in.Text)
    if err != nil {
        return nil, err
    }
    return &pb.Message{Id : message.ID, TopicId: message.TopicID, UserId: message.UserID, Text: message.Text, CreatedAt: timestamppb.New(message.CreatedAt), Likes: message.Likes}, nil
}

// UpdateMessage je pisalna metoda, ki posodobi obstoječe sporočilo
func (s *Server) UpdateMessage(ctx context.Context, in *pb.UpdateMessageRequest) (*pb.Message, error) {
    if !s.nodeState.IsHead() {
        fmt.Println("server.go: UpdateMessage() error: writes are only allowed on head node!")
        return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
    } 
    message, err := s.storage.UpdateMessage(in.TopicId, in.UserId, in.MessageId, in.Text)
    if err != nil {
        return nil, err
    }
    return &pb.Message{Id : message.ID, TopicId: message.TopicID, UserId: message.UserID, Text: message.Text, CreatedAt: timestamppb.New(message.CreatedAt), Likes: message.Likes}, nil
}

// DeleteMessage je pisalna metoda, ki izbrise obstoječe sporočilo
func (s *Server) DeleteMessage(ctx context.Context, in *pb.DeleteMessageRequest) (*emptypb.Empty, error) {
    if !s.nodeState.IsHead() {
        fmt.Println("server.go: DeleteMessage() error: writes are only allowed on head node!")
        return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
    } 
    err := s.storage.DeleteMessage(in.TopicId, in.UserId, in.MessageId)
    if err != nil {
        return nil, err
    }
    return &emptypb.Empty{}, nil
}

// LikeMessage je pisalna metoda, ki vsečka sporočilo
func (s *Server) LikeMessage(ctx context.Context, in *pb.LikeMessageRequest) (*pb.Message, error) {
    if !s.nodeState.IsHead() {
        fmt.Println("server.go: LikeMessage() error: writes are only allowed on head node!")
        return nil, status.Errorf(codes.FailedPrecondition, "writes allowed only on head node")
    } 
    message, err := s.storage.LikeMessage(in.TopicId, in.MessageId, in.UserId)
    if err != nil {
        return nil, err
    }
    return &pb.Message{Id : message.ID, TopicId: message.TopicID, UserId: message.UserID, Text: message.Text, CreatedAt: timestamppb.New(message.CreatedAt), Likes: message.Likes}, nil
}

func (s *Server) ListTopics(ctx context.Context,_ *emptypb.Empty) (*pb.ListTopicsResponse, error) {
    topics, err := s.storage.ListTopics()
    if err != nil {
        return nil, err
    }
    response := &pb.ListTopicsResponse{}
    for _, topic := range topics {
        response.Topics = append(response.Topics, &pb.Topic{Id: topic.ID, Name: topic.Name })
    }
    return response, nil
}

func (s *Server) GetMessages(ctx context.Context,in *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
    msgs, err := s.storage.GetMessages(in.TopicId, in.FromMessageId, in.Limit)
    if err != nil {
        return nil, err
    }

    response := &pb.GetMessagesResponse{}
    for _, m := range msgs {
        response.Messages = append(response.Messages, &pb.Message{Id: m.ID, TopicId: m.TopicID, UserId: m.UserID, Text: m.Text, CreatedAt: timestamppb.New(m.CreatedAt), Likes: m.Likes, })
    }
    return response, nil
}
