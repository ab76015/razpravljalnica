package storage

import (
    "errors"
    "sync"
    "time"  
    "fmt"
    "log"
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

func (m *MemStorage) debugDumpLocked() string {
    var max uint64
    committedCount := 0
    for w := range m.writes {
        if w > max {
            max = w
        }
        if c, ok := m.committed[w]; ok && c {
            committedCount++
        }
    }
    return fmt.Sprintf("writes=%d msgWrites=%d messages=%d committed=%d maxWrite=%d",
        len(m.writes), len(m.msgWrite), len(m.messages), committedCount, max)
}

// DebugDump is a thread-safe wrapper if you want to log outside the lock.
func (m *MemStorage) DebugDump() string {
    m.mu.Lock()
    defer m.mu.Unlock()
    return m.debugDumpLocked()
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
    // idempotence: if this write already applied, return recorded message (may be nil for deletes)
    if existing, exists := m.writes[writeID]; exists {
        return existing, nil
    }

    // Make Message.ID deterministic and global: derive it from writeID.
    // This ensures every replica assigns the same message id for the POST.
    msgID := int64(writeID)
    msg := &Message{
        ID:        msgID,
        TopicID:   topicID,
        UserID:    userID,
        Text:      text,
        CreatedAt: time.Now(),
        Likes:     0,
    }
    // store by the globally-determined id
    m.messages[msgID] = msg
    m.msgWrite[msgID] = writeID
    m.writes[writeID] = msg
    m.committed[writeID] = false
    // ensure nextMsgID remains >= max(message ids seen)+1 to keep GetMessages loop safe
    if msgID >= m.nextMsgID {
        m.nextMsgID = msgID + 1
    }
    // compact debug dump while still holding lock
    dump := m.debugDumpLocked()
    log.Printf("[STORAGE][POST] write=%d msg=%d topic=%d user=%d dump=%s\n", writeID, msg.ID, topicID, userID, dump)

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
// CRAQ
// canonical signature: topicID, msgID, userID, writeID
func (m *MemStorage) DeleteMessageWithWriteID(topicID, msgID, userID int64, writeID uint64) (*Message, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    // If message already removed but tombstoned -> idempotent success
    if delW, wasDeleted := m.deleted[msgID]; wasDeleted {
        // record this write mapping for bookkeeping (idempotence)
        m.msgWrite[msgID] = writeID
        // keep a nil placeholder to indicate this write exists but payload not present
        m.writes[writeID] = nil
        m.committed[writeID] = false
        log.Printf("[STORAGE][DELETE] idempotent delete msg=%d topic=%d user=%d already tombstoned at write=%d; newWrite=%d\n",
            msgID, topicID, userID, delW, writeID)
        return nil, nil
    }

    msg, ok := m.messages[msgID]
    if !ok {
        // not found and not tombstoned -> helpful diagnostic
        log.Printf("[STORAGE][DELETE] message not found msg=%d reqTopic=%d reqUser=%d\n", msgID, topicID, userID)
        return nil, errors.New("message not found")
    }

    if msg.TopicID != topicID {
        // log full diagnostic so we can see which IDs got swapped
        log.Printf("[STORAGE][DELETE] topic mismatch: reqTopic=%d msg.Topic=%d msgID=%d reqUser=%d msgUser=%d\n",
            topicID, msg.TopicID, msgID, userID, msg.UserID)
        return nil, errors.New("message not in topic")
    }

    if msg.UserID != userID {
        return nil, errors.New("incorrect user id")
    }

    // idempotence: if this exact write was already applied, return current state
    if _, exists := m.writes[writeID]; exists {
        return msg, nil
    }

    // perform delete: remove likes, mark tombstone, keep bookkeeping for CRAQ
    for like := range m.likes {
        if like.MessageID == msgID {
            delete(m.likes, like)
        }
    }

    // remove from messages map (logical delete on apply)
    delete(m.messages, msgID)

    m.msgWrite[msgID] = writeID
    // store the message object in writes so MarkCommitted can still return it for event emission
    m.writes[writeID] = msg
    m.committed[writeID] = false
    m.deleted[msgID] = writeID

    log.Printf("[STORAGE][DELETE] applied delete msg=%d topic=%d by user=%d write=%d\n", msgID, topicID, userID, writeID)
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

func (m *MemStorage) ListUsers() ([]*User, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    out := make([]*User, 0, len(m.users))
    for _, u := range m.users {
        out = append(out, u)
    }
    return out, nil
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
// MarkCommitted nastavi writeID kot commited in vrne message.
func (m *MemStorage) MarkCommitted(writeID uint64) (*Message, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    msg, ok := m.writes[writeID]
    if !ok {
        return nil, errors.New("write id not found")
    }

    // handle the case where the write has a nil payload (idempotent delete placeholder)
    if msg == nil {
        // find message id from deleted mapping (the tombstone maps msgID -> writeID)
        var foundMsgID int64 = -1
        for mid, dw := range m.deleted {
            if dw == writeID {
                foundMsgID = mid
                break
            }
        }
        if foundMsgID == -1 {
            // cannot determine msgID; return error (shouldn't happen)
            return nil, errors.New("write id has nil payload and no deleted mapping")
        }

        // finalise delete: likes already removed at apply; clean up tombstone bookkeeping
        for like := range m.likes {
            if like.MessageID == foundMsgID {
                delete(m.likes, like)
            }
        }
        // message was already removed from m.messages at apply time
        delete(m.deleted, foundMsgID)
        delete(m.msgWrite, foundMsgID)

        m.committed[writeID] = true
        log.Printf("[STORAGE][COMMIT] write=%d msg=%d committed=true (nil-payload delete finish)\n", writeID, foundMsgID)
        return nil, nil
    }

    // normal (non-nil) payload path
    msgID := msg.ID
    // If this write is the tombstone for a previously deleted message, complete final removal
    if delW, isDel := m.deleted[msgID]; isDel && delW == writeID {
        // delete likes (defensive)
        for like := range m.likes {
            if like.MessageID == msgID {
                delete(m.likes, like)
            }
        }
        // physically remove (if present) and clean bookkeeping
        delete(m.messages, msgID)
        delete(m.deleted, msgID)
        delete(m.msgWrite, msgID)
    }

    // snapshot for logging
    dump := m.debugDumpLocked()
    log.Printf("[STORAGE][COMMIT] write=%d msg=%d committed=true dump=%s\n", writeID, msgID, dump)
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


