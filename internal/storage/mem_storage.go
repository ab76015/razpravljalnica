package storage


import (
    "errors"
    "sync"
    "time"    
)

type MemStorage struct {
    mu sync.Mutex
    users map[int64]*User
    topics map[int64]*Topic
    messages map[int64]*Message
    nextUserID int64
    nextTopicID int64
    nextMsgID int64
}

func NewMemStorage() *MemStorage {
    return &MemStorage{
        users: make(map[int64]*User),
        topics: make(map[int64]*Topic),
        messages: make(map[int64]*Message),
        nextUserID:  1,
        nextTopicID: 1,
        nextMsgID:   1,
    }
}

func (m *MemStorage) CreateUser(name string) (*User, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    u := &User{
        ID:   m.nextUserID,
        Name: name,
    }
    m.users[m.nextUserID] = u
    m.nextUserID++
    return u, nil
}

func (m *MemStorage) CreateTopic(name string) (*Topic, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    t := &Topic{
        ID:   m.nextTopicID,
        Name: name,
    }
    m.topics[m.nextTopicID] = t
    m.nextTopicID++
    return t, nil
}

func (m *MemStorage) PostMessage(topicID, userID int64, text string) (*Message, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    if _, ok := m.topics[topicID]; !ok {
        return nil, errors.New("topic not found")
    }
    if _, ok := m.users[userID]; !ok {
        return nil, errors.New("user not found")
    }

    msg := &Message{
        ID:        m.nextMsgID,
        TopicID:   topicID,
        UserID:    userID,
        Text:      text,
        CreatedAt: time.Now(),
        Likes:     0,
    }
    m.messages[m.nextMsgID] = msg
    m.nextMsgID++
    return msg, nil
}

func (m *MemStorage) UpdateMessage(topicID, userID, msgID int64, text string) (*Message, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    msg, ok := m.messages[msgID];
    if !ok {
        return nil, errors.New("message not found")
    }
    if msg.TopicID != topicID {
        return nil, errors.New("message not in topic")
    }
    if msg.UserID != userID {
        return nil, errors.New("incorrect user id")
    }

    msg.Text = text
    return msg, nil
}

func (m *MemStorage) DeleteMessage(topicID, userID, msgID int64) (*Message, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    msg, ok := m.messages[msgID];
    if !ok {
        return nil, errors.New("message not found")
    }
    if msg.TopicID != topicID {
        return nil, errors.New("message not in topic")
    }
    if msg.UserID != userID {
        return nil, errors.New("incorrect user id")
    }

    delete(m.messages, msgID)
    return nil, nil
}

