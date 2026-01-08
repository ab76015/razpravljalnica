package server

import (
	"context"
	pb "github.com/ab76015/razpravljalnica/api/pb"
	"github.com/ab76015/razpravljalnica/internal/replication"
	"github.com/ab76015/razpravljalnica/internal/storage"
    "github.com/ab76015/razpravljalnica/internal/subscription"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Subscription hkrani vse naročnine (imajo vsi dataplane serverji)
type Subscription struct {
    userID int64
    topics map[int64]bool
    stream pb.MessageBoard_SubscribeTopicServer
}

type Server struct {
	pb.UnimplementedMessageBoardServer
	storage     storage.Storage
	replication *replication.DataNodeServer
    subsMu      sync.RWMutex
    subscriptions map[string]*Subscription // key = stream ID
}

func NewMessageBoardServer(s storage.Storage, r *replication.DataNodeServer) *Server {
	return &Server{storage: s, replication: r, subscriptions: make(map[string]*Subscription),}
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

// GetSubscriptionNode (HEAD ONLY) izbere subscribe vozlisce za userja in mu vse info zakodira in vrne kot token
func (s *MessageBoardServer) GetSubscriptionNode(ctx context.Context,req *pb.SubscriptionNodeRequest,) (*pb.SubscriptionNodeResponse, error) {
    // samo glava določi kam se lahko subscriba
    if !s.replication.IsHead() {
        return nil, status.Error(codes.FailedPrecondition, "not head")
    }
    // izberi konkreten subscribe node
    nodes := s.replication.AllNodes()
    node := subscriptions.SelectNode(nodes, req.UserId)
    // ustvari subscribtion grant
    grant := &subscriptions.Grant{
        NodeID:  node.NodeId,
        UserID:  req.UserId,
        Topics:  req.TopicId,
        Expires: time.Now().Add(30 * time.Minute),
    }
    // grant zakodiraj v token, za lažji prenos
    token, err := subscriptions.Encode(grant)
    if err != nil {
        return nil, status.Error(codes.Internal, err.Error())
    }
    // vrni token in subscribe node
    return &pb.SubscriptionNodeResponse{
        SubscribeToken: token,
        Node:           node,
    }, nil
}

// SubscribeTopic omogoča klientu da se naroči na topic preko tokena, ki ga je prejel
func (s *MessageBoardServer) SubscribeTopic(req *pb.SubscribeTopicRequest, stream pb.MessageBoard_SubscribeTopicServer,) error {
    // dekodiraj subscribe token nazaj v json struct (glej subscribtion/grant.go)
    grant, err := subscriptions.Decode(req.SubscribeToken)
    if err != nil {
        return status.Error(codes.PermissionDenied, "invalid token")
    }
    // preveri ujemanje nodeid
    if grant.NodeID != s.replication.State().Self().NodeId  {
        return status.Error(codes.PermissionDenied, "wrong node")
    }
    // preveri ujemanje userid
    if grant.UserID != req.UserId {
        return status.Error(codes.PermissionDenied, "user mismatch")
    }
    // preveri ali se je token iztekel
    if time.Now().After(grant.Expires) {
        return status.Error(codes.PermissionDenied, "token expired")
    }
    // preveri dovoljene topics
    allowed := make(map[int64]bool)
    for _, t := range grant.Topics {
        allowed[t] = true
    }
    for _, t := range req.TopicId {
        if !allowed[t] {
            return status.Error(codes.PermissionDenied, "topic not allowed")
        }
    }
    // registriraj narocnika
    sub := &Subscription{
        userID: req.UserId,
        topics: allowed,
        stream: stream,
    }
    // globalno unikaten identifier
    subID := uuid.NewString()

    s.subsMu.Lock()
    s.subscriptions[subID] = sub
    s.subsMu.Unlock()

    defer func() {
        s.subsMu.Lock()
        delete(s.subscriptions, subID)
        s.subsMu.Unlock()
    }()

    // blokiraj dokler se client ne disconnect-a
    <-stream.Context().Done()
    return nil
}

func (s *MessageBoardServer) emitEvent(ev *pb.MessageEvent) {
    s.subsMu.RLock()
    defer s.subsMu.RUnlock()
    for _, sub := range s.subscriptions {
        if sub.topics[ev.Message.TopicId] {
            sub.stream.Send(ev)
        }
    }
}

