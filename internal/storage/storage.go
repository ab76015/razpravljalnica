package storage

import (  
    //"errors"  
    "time"
)


type User struct {
    ID  int64
    Name string
}

type Topic struct {
    ID  int64
    Name string
}

type Message struct {
    ID int64  
    TopicID int64
    UserID int64
    Text string
    CreatedAt time.Time
    Likes int32
}

type Like struct {
    TopicID int64
    MessageID int64
    UserID int64
}

type Storage interface {
    CreateUser(name string) (*User, error)
    CreateTopic(name string) (*Topic, error)
    PostMessage(topicID int64, userID int64, text string) (*Message, error)
    UpdateMessage(topicID int64, userID int64, msgID int64, text string) (*Message, error)
    DeleteMessage(topicID int64, userID int64, msgID int64) error
    LikeMessage(topicID int64, msgID int64, userID int64) (*Message, error)
    ListTopics() ([]*Topic, error)
    GetMessages(topicID int64, fromMsgID int64, limit int32) ([]*Message, error)
}

