package peers

import "github.com/tonyyanga/gdp-replicate/gdplogd"
import "github.com/tonyyanga/gdp-replicate/policy"

// Entry point to talk to other replication daemons
type ReplicateNetworkMgr interface {
    // TODO add more init parameters, e.g. peers list
    Init()

    // handler for incoming messages
    ListenAndServe(address string, handler func(ResponseWriter, *Request))

    Send(peer gdplogd.HashAddr, msg policy.Message) error

    Broadcast(msg, policy.Message) map[gdplogd.HashAddr]error
}
