package policy

import (
    "io"

    "github.com/tonyyanga/gdp-replicate/gdplogd"
)

// Return all connected hash addresses in the graph from a list of requested
// This function should handle deduplication
func (policy *GraphDiffPolicy) getConnectedAddrs(addrs []gdplogd.HashAddr) []gdplogd.HashAddr {

}

// Write a data section, not including "data\n", from a list of hash addresses requested
// Returns an error if some of the hash address are not available
func (policy *GraphDiffPolicy) constructDataSection(addrs []gdplogd.HashAddr, dest io.Writer) error {
    // TODO
}

// Process data section of the message
// Assume that "data\n" is already consumed
func (policy *GraphDiffPolicy) processDataSection(body io.Reader) {
    // TODO
}

// Try to store the data at addr
// Return whether the data is stored via this call, or already in gdplogd
func (policy *GraphDiffPolicy) tryStoreData(addr gdplogd.HashAddr, data []byte) bool {
    // TODO
}
