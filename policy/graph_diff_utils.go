package policy

import (
    "io"

    "github.com/tonyyanga/gdp-replicate/gdplogd"
)

// Get peer policy context
func (policy *GraphDiffPolicy) getPeerPolicyContext(peer gdplogd.HashAddr) *peerPolicyContext {

}

// Return all connected hash addresses in the graph from a list of requested
// This function should handle deduplication
func (ctx *peerPolicyContext) getConnectedAddrs(addrs []gdplogd.HashAddr) []gdplogd.HashAddr {

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

}
