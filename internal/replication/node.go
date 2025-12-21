package replication

import (
    pb "github.com/ab76015/razpravljalnica/api/pb"
    "github.com/ab76015/razpravljalnica/internal/storage"
)

type Node struct {
    Info *pb.NodeInfo

    IsHead bool
    IsTail bool

    Store storage.Storage
    Next  *Node // naslednji v verigi (nil če tail)
}

