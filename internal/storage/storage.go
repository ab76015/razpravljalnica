package storage

import (  
    "errors"  
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

type Storage interface {
    CreateUser(name string) (*User, error)
    CreateTopic(name string) (*Topic, error)
    PostMessage(topicID int64, userID int64, text string) 
    //...add more methods below
}

