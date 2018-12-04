package policy

import (
    "io"

    "github.com/tonyyanga/gdp-replicate/gdplogd"
)

// Get peer policy context
func (policy *GraphDiffPolicy) getPeerPolicyContext(peer gdplogd.HashAddr) *peerPolicyContext {
    return &peerPolicyContext {
        conn: policy.conn,
        graph: policy.graphInUse[peer],
        policy: policy,
    }
}

// Return all connected hash addresses in the graph from a list of requested
// This function should handle deduplication
func (ctx *peerPolicyContext) getConnectedAddrs(addrs []gdplogd.HashAddr) []gdplogd.HashAddr {
    empty := []gdplogd.HashAddr{}

    result := make(map[gdplogd.HashAddr]int)

    _getConnected := func(addr gdplogd.HashAddr) {
        // Add addr itself
        result[addr] = 1

        prev, _ := ctx.searchAhead(addr, empty)
        for _, node := range prev {
            result[node] = 1
        }

        next, _ := ctx.searchAfter(addr, empty)
        for _, node := range next {
            result[node] = 1
        }
    }

    for _, addr := range addrs {
        _getConnected(addr)
    }

    ret := []gdplogd.HashAddr{}
    for key := range result {
        ret = append(ret, key)
    }

    return ret
}

// Write a data section, not including "data\n", from a list of hash addresses requested
// Returns an error if some of the hash address are not available
func (ctx *peerPolicyContext) constructDataSection(addrs []gdplogd.HashAddr, dest io.Writer) error {
    // TODO
}

// Process data section of the message and update the current graph accordingly
// Assume that "data\n" is already consumed
func (ctx *peerPolicyContext) processDataSection(body io.Reader) {
    // TODO
}

// Try to store the data at addr
// Return whether the data is stored via this call, or already in gdplogd
func (ctx *peerPolicyContext) tryStoreData(addr gdplogd.HashAddr, data []byte) bool {
    // TODO
    return true
}

// Traverse ahead in the graph starting from "start". Traversal on a certain path ends when meeting a node in
// "terminals"
// Return:
//   a list of hash addresses visited, not including start or terminals
//   a list of begins / ends in local graph reached
func (ctx *peerPolicyContext) searchAhead(start gdplogd.HashAddr, terminals []gdplogd.HashAddr) ([]gdplogd.HashAddr, []gdplogd.HashAddr) {

}

// Traverse after in the graph starting from "start". Traversal on a certain path ends when meeting a node in
// "terminals"
// Return:
//   a list of hash addresses visited, not including start or terminals
//   a list of begins / ends in local graph reached
func (ctx *peerPolicyContext) searchAfter(start gdplogd.HashAddr, terminals []gdplogd.HashAddr) ([]gdplogd.HashAddr, []gdplogd.HashAddr) {

}

// Compare peer's begins and ends with my own
// Return in the following order:
//   local begins not matched
//   local ends not matched
//   peer begins not matched
//   peer ends not matched
func (ctx *peerPolicyContext) compareBeginsEnds(peerBegins, peerEnds []gdplogd.HashAddr) ([]gdplogd.HashAddr, []gdplogd.HashAddr, []gdplogd.HashAddr, []gdplogd.HashAddr) {
    localBegins := ctx.graph.GetLogicalBegins()
    localEnds := ctx.graph.GetLogicalEnds()

    diffSlices := func(local, peer []gdplogd.HashAddr) ([]gdplogd.HashAddr, []gdplogd.HashAddr) {
        sliceToMaps := func(s []gdplogd.HashAddr) map[gdplogd.HashAddr]int {
            result := make(map[gdplogd.HashAddr]int)
            for _, item := range s {
                result[item] = 1
            }
            return result
        }

        localMap := sliceToMaps(local)
        peerMap := sliceToMaps(peer)

        localDiff := make([]gdplogd.HashAddr, 0)
        peerDiff := make([]gdplogd.HashAddr, 0)

        for _, l := range local {
            if _, ok := peerMap[l]; !ok {
                localDiff = append(localDiff, l)
            }
        }

        for _, l := range peer {
            if _, ok := localMap[l]; !ok {
                peerDiff = append(peerDiff, l)
            }
        }

        return localDiff, peerDiff
    }

    localBeginsRet, peerBeginsRet := diffSlices(localBegins, peerBegins)
    localEndsRet, peerEndsRet := diffSlices(localEnds, peerEnds)

    return localBeginsRet, localEndsRet, peerBeginsRet, peerEndsRet
}
