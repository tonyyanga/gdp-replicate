package peers

import (
    "github.com/tonyyanga/gdp-replicate/gdplogd"
    "github.com/tonyyanga/gdp-replicate/policy"
)

// Simple Replication Manager that directly connects to peers
type SimpleReplicateMgr {
    // Store the IP:Port address for each peer
    PeerAddrMap map[HashAddr]string
}

// Constructor for SimpleReplicateMgr
func NewSimpleReplicateMgr(peerAddrMap map[HashAddr]string) *SimpleReplicateMgr {
    return &SimpleReplicateMgr{
        PeerAddrMap: peerAddrMap
    }
}

func (mgr *SimpleReplicateMgr) ListenAndServe(address string, handler func(msg *policy.Message)) error {
    // TODO
}

func (mgr *SimpleReplicateMgr) Send(peer gdplogd.HashAddr, msg *policy.Message) error {
    // TODO
}

func (mgr *SimpleReplicateMgr) Broadcast(msg *policy.Message) map[gdplogd.HashAddr]error {
    // TODO
}


