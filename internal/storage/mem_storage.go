package storage

import (
    "errors"
    "sync"
    "time"  
//    "log"
)

type MemStorage struct {
    mu sync.Mutex
    users map[int64]*User
    topics map[int64]*Topic
    messages map[int64]*Message
    likes map[Like]struct{}
    nextUserID int64
    nextTopicID int64
    nextMsgID int64
    deleted map[int64]uint64
    // CRAQ
    // writeID -> message pointer
    writes map[uint64]*Message
    // msgID -> writeID (obratni map)
    msgWrite map[int64]uint64
    // committed map za writeID -> bool
    committed map[uint64]bool
}

func NewMemStorage() *MemStorage {
    return &MemStorage{
        users: make(map[int64]*User),
        topics: make(map[int64]*Topic),
        messages: make(map[int64]*Message),
        likes: make(map[Like]struct{}),
        nextUserID:  0,
        nextTopicID: 0,
        nextMsgID:   0,
        deleted: make(map[int64]uint64), 
        // CRAQ
        writes:    make(map[uint64]*Message),
        msgWrite:  make(map[int64]uint64),
        committed: make(map[uint64]bool),
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

// CRAQ, nastavi msg kot 'dirty' sprva
func (m *MemStorage) PostMessageWithWriteID(topicID, userID int64, text string, writeID uint64) (*Message, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    /*
    log.Printf(
        "[DEBUG][PostMessage] topicID=%d userID=%d\n",
        topicID, userID,
    )

    // dump topics
    log.Printf("[DEBUG][PostMessage] topics in storage:")
    for id, t := range m.topics {
        log.Printf("  topic id=%d name=%q", id, t.Name)
    }

    // dump users
    log.Printf("[DEBUG][PostMessage] users in storage:")
    for id, u := range m.users {
        log.Printf("  user id=%d name=%q", id, u.Name)
    }*/

    if _, ok := m.topics[topicID]; !ok {
        return nil, errors.New("topic not found")
    }
    if _, ok := m.users[userID]; !ok {
        return nil, errors.New("user not found")
    }
    // ze bil apliciran, idempotenca
    if _, exists := m.writes[writeID]; exists {
        return m.writes[writeID], nil
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
    m.msgWrite[m.nextMsgID] = writeID
    m.writes[writeID] = msg
    m.committed[writeID] = false
    m.nextMsgID++
    return msg, nil
}

// deprecated
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

// CRAQ verzija
func (m *MemStorage) UpdateMessageWithWriteID(topicID, userID, msgID int64, text string, writeID uint64) (*Message, error) {
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
    
    // ze bil apliciran, idempotenca
    if _, exists := m.writes[writeID]; exists {
        return msg, nil
    }

    msg.Text = text
    // craq
    m.msgWrite[msgID] = writeID
    m.writes[writeID] = msg
    m.committed[writeID] = false
    return msg, nil 
}

// deprecated
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

// CRAQ
func (m *MemStorage) DeleteMessageWithWriteID(topicID, userID, msgID int64, writeID uint64) (*Message, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    msg, ok := m.messages[msgID];
    if !ok {
        return msg, errors.New("message not found")
    }
    if msg.TopicID != topicID {
        return msg, errors.New("message not in topic")
    }
    if msg.UserID != userID {
        return msg, errors.New("incorrect user id")
    }
    
    // ze bil apliciran (idempotenca)
    if _, exists := m.writes[writeID]; exists {
        return msg, nil
    }
    
    /*zbrisemo se vse like s tem msgID
    for like := range m.likes {
        if like.MessageID == msgID {
            delete(m.likes, like)
        }
    }*/

    delete(m.messages, msgID)
    // craq
    m.msgWrite[msgID] = writeID
    m.writes[writeID] = msg 
    m.committed[writeID] = false
    //tombstone map, da lazje prepoznamo delete ob commit casu
    m.deleted[msgID] = writeID
    return msg, nil
}
// deprecated
func (m *MemStorage) DeleteMessage(topicID, userID, msgID int64) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    msg, ok := m.messages[msgID];
    if !ok {
        return errors.New("message not found")
    }
    if msg.TopicID != topicID {
        return errors.New("message not in topic")
    }
    if msg.UserID != userID {
        return errors.New("incorrect user id")
    }
    
    //zbrisemo se vse like s tem msgID
    for like := range m.likes {
        if like.MessageID == msgID {
            delete(m.likes, like)
        }
    }

    delete(m.messages, msgID)
    return nil
}

// CRAQ
func (m* MemStorage) LikeMessageWithWriteID(topicID, msgID, userID int64, writeID uint64) (*Message, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    if _, ok := m.topics[topicID]; !ok {
        return nil, errors.New("topic not found")
    }
    if _, ok := m.users[userID]; !ok {
        return nil, errors.New("user not found")
    }

    msg, ok := m.messages[msgID];
    if !ok {
        return nil, errors.New("message not found")
    }
    if msg.TopicID != topicID {
        return nil, errors.New("message not in topic")
    }

    like := Like{
        TopicID:     topicID,
        MessageID:   msgID,
        UserID:      userID,
    }
    // Ce je tocno ta writeID ze bil apliciran, vrni trenutno stanje (idempotentnost)
    if _, exists := m.writes[writeID]; exists {
        return msg, nil
    }

    m.likes[like] = struct{}{}
    msg.Likes++
    //craq
    m.msgWrite[msgID] = writeID
    m.writes[writeID] = msg
    m.committed[writeID] = false

    return msg, nil
}

// deprecated
func (m* MemStorage) LikeMessage(topicID, msgID, userID int64) (*Message, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    if _, ok := m.topics[topicID]; !ok {
        return nil, errors.New("topic not found")
    }
    if _, ok := m.users[userID]; !ok {
        return nil, errors.New("user not found")
    }

    msg, ok := m.messages[msgID];
    if !ok {
        return nil, errors.New("message not found")
    }
    if msg.TopicID != topicID {
        return nil, errors.New("message not in topic")
    }

    like := Like{
        TopicID:     topicID,
        MessageID:   msgID,
        UserID:      userID,
    }

    if _, exists := m.likes[like]; exists {
        return nil, errors.New("message already liked")
    }

    m.likes[like] = struct{}{}
    msg.Likes++

    return msg, nil
}

func (m* MemStorage) ListTopics() ([]*Topic, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    topics := make([]*Topic, 0, len(m.topics))
    for _, topic := range m.topics {
        topics = append(topics, topic)
    }
    return topics, nil
}

func (m *MemStorage) GetMessages(topicID, fromMsgID int64, limit int32) ([]*Message, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    if _, ok := m.topics[topicID]; !ok {
        return nil, errors.New("topic not found")
    }

    if limit <= 0 {
        return nil, errors.New("limit less than 1")
    }

    messages := []*Message{}
    for id := fromMsgID + 1; id < m.nextMsgID; id++ {
        msg, ok := m.messages[id]
        if !ok || msg.TopicID != topicID {
            continue
        }
        messages = append(messages, msg)
        if int32(len(messages)) >= limit {
            break
        }
    }
    return messages, nil
}


// MarkCommitted nastavi writeID kot commited in vrne message.
func (m *MemStorage) MarkCommitted(writeID uint64) (*Message, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    msg, ok := m.writes[writeID]
    if !ok {
        return nil, errors.New("write id not found")
    }

    msgID := msg.ID
    // Ce je ta write v oznacen kot tombstone v deleted, ga dokoncno odstrani
    if delW, isDel := m.deleted[msgID]; isDel && delW == writeID {
        // izbrisi tudi vse njegove like
        for like := range m.likes {
            if like.MessageID == msgID {
                delete(m.likes, like)
            }
        }
        // dokoncno izbrisi
        delete(m.messages, msgID)
        delete(m.deleted, msgID)
        delete(m.msgWrite, msgID)
    }

    m.committed[writeID] = true
    return msg, nil
}

// GetCommittedMessages returns committed messages (ID > fromMsgID) for topic.
func (m *MemStorage) GetCommittedMessages(topicID, fromMsgID int64, limit int32) ([]*Message, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    if _, ok := m.topics[topicID]; !ok {
        return nil, errors.New("topic not found")
    }

    if limit < 0 {
        limit = 0
    }

    out := []*Message{}
    for id := fromMsgID + 1; id < m.nextMsgID; id++ {
        msg, ok := m.messages[id]
        if !ok || msg.TopicID != topicID {
            continue
        }
        writeID, ok := m.msgWrite[id]
        if !ok {
            continue
        }
        committed, ok := m.committed[writeID]
        if !ok || !committed {
            continue
        }
        out = append(out, msg)
        if limit > 0 && int32(len(out)) >= limit {
            break
        }
    }
    return out, nil
}


