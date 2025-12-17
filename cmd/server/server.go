package main

import (
    "context"
    "google.golang.org/protobuf/types/known/emptypb"
    "google.golang.org/protobuf/types/known/timestamppb"
    pb "github.com/ab76015/razpravljalnica/api/pb"
    "github.com/ab76015/razpravljalnica/internal/storage"
)

type Server struct {
    pb.UnimplementedMessageBoardServer
    storage storage.Storage
}

func NewServer(s storage.Storage) *Server {
    return &Server{storage: s}
}

func (s *Server) CreateUser(ctx context.Context, in *pb.CreateUserRequest) (*pb.User, error) {
    user, err := s.storage.CreateUser(in.Name)
    if err != nil {
        return nil, err
    }
    return &pb.User{Id: user.ID, Name: user.Name}, nil
}

func (s *Server) CreateTopic(ctx context.Context, in *pb.CreateTopicRequest) (*pb.Topic, error) {
    topic, err := s.storage.CreateTopic(in.Name)
    if err != nil {
        return nil, err
    }
    return &pb.Topic{Id: topic.ID, Name: topic.Name }, nil
}

func (s *Server) PostMessage(ctx context.Context, in *pb.PostMessageRequest) (*pb.Message, error) {
    message, err := s.storage.PostMessage(in.TopicId, in.UserId, in.Text)
    if err != nil {
        return nil, err
    }
    return &pb.Message{Id : message.ID, TopicId: message.TopicID, UserId: message.UserID, Text: message.Text, CreatedAt: timestamppb.New(message.CreatedAt), Likes: message.Likes}, nil
}

func (s *Server) UpdateMessage(ctx context.Context, in *pb.UpdateMessageRequest) (*pb.Message, error) {
    message, err := s.storage.UpdateMessage(in.TopicId, in.UserId, in.MessageId, in.Text)
    if err != nil {
        return nil, err
    }
    return &pb.Message{Id : message.ID, TopicId: message.TopicID, UserId: message.UserID, Text: message.Text, CreatedAt: timestamppb.New(message.CreatedAt), Likes: message.Likes}, nil
}

func (s *Server) DeleteMessage(ctx context.Context, in *pb.DeleteMessageRequest) (*emptypb.Empty, error) {
    err := s.storage.DeleteMessage(in.TopicId, in.UserId, in.MessageId)
    if err != nil {
        return nil, err
    }
    return &emptypb.Empty{}, nil
}

func (s *Server) LikeMessage(ctx context.Context, in *pb.LikeMessageRequest) (*pb.Message, error) {
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
