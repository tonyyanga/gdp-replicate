package peers

import (
    "github.com/tonyyanga/gdp-replicate/gdplogd"
    "github.com/tonyyanga/gdp-replicate/policy"
)

// Entry point to talk to other replication daemons
type ReplicateNetworkMgr interface {
    // handler for incoming messages
    ListenAndServe(address string, handler func(msg *policy.Message)) error

    Send(peer gdplogd.HashAddr, msg *policy.Message) error

    Broadcast(msg *policy.Message) map[gdplogd.HashAddr]error
}
