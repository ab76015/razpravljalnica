package subscription

import "time"

type Grant struct {
    NodeID   string
    UserID   int64
    Topics   []int64
    Expires time.Time
}


