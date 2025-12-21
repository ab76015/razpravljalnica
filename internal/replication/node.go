package replication

import (
    pb "github.com/ab76015/razpravljalnica/api/pb"
    "github.com/ab76015/razpravljalnica/internal/storage"
)

const (
    Head   = 0
    Middle = 1
    Tail   = 2
)

type Node struct {
    Info *pb.NodeInfo

    Role int // 0=head, 1=middle, 2=tail

    Store storage.Storage
    Next  pb.ReplicationClient
}

