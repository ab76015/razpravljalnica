package subscription

import (
    pb "github.com/ab76015/razpravljalnica/api/pb"
)


// SelectNode na podlagi userID preslika/izbere subscribe node
func SelectNode(nodes []*pb.NodeInfo, userID int64) *pb.NodeInfo {
    if len(nodes) == 0 {
        return nil
    }
    idx := int(userID % int64(len(nodes)))
    return nodes[idx]
}

