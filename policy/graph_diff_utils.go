package policy

import (
    "io"
    "fmt"
    "bytes"
    "bufio"

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

// Process data section of the message and update the current graph accordingly
// Assume that "data\n" is already consumed
func (policy *GraphDiffPolicy) processDataSection(body io.Reader) {
    // TODO
}

// Try to store the data at addr
// Return whether the data is stored via this call, or already in gdplogd
func (policy *GraphDiffPolicy) tryStoreData(addr gdplogd.HashAddr, data []byte) bool {
    // TODO
}

// Traverse ahead in the graph starting from "start". Traversal on a certain path ends when meeting a node in
// "terminals"
// Return:
//   a list of hash addresses visited, not including start or terminals
//   a list of begins / ends in local graph reached
func (policy *GraphDiffPolicy) searchAhead(start gdplogd.HashAddr, terminals []gdplogd.HashAddr) ([]gdplogd.HashAddr, []gdplogd.HashAddr) {

}

// Traverse after in the graph starting from "start". Traversal on a certain path ends when meeting a node in
// "terminals"
// Return:
//   a list of hash addresses visited, not including start or terminals
//   a list of begins / ends in local graph reached
func (policy *GraphDiffPolicy) searchAfter(start gdplogd.HashAddr, terminals []gdplogd.HashAddr) ([]gdplogd.HashAddr, []gdplogd.HashAddr) {

}

// Process begins and ends section, includes "begins\n", "ends\n"
func (policy *GraphDiffPolicy) processBeginsEnds(body io.Reader) ([]gdplogd.HashAddr, []gdplogd.HashAddr, error) {
    reader := bufio.NewReader(body)
    line, err := reader.ReadBytes('\n')

    if err != nil || bytes.Compare(line, []byte("begins\n")) != 0 {
        return nil, nil, fmt.Errorf("Error processing message: begins")
    }

    peerBegins, err := addrListFromReader(reader)
    if err != nil {
        return nil, nil, err
    }

    line, err = reader.ReadBytes('\n')

    if err != nil || bytes.Compare(line, []byte("ends\n")) != 0 {
        return nil, nil, fmt.Errorf("Error processing message: ends")
    }

    peerEnds, err := addrListFromReader(reader)
    if err != nil {
        return nil, nil, err
    }

    return peerBegins, peerEnds, nil
}

// Compare peer's begins and ends with my own
// Return in the following order:
//   local begins not matched
//   local ends not matched
//   peer begins not matched
//   peer ends not matched
func (policy *GraphDiffPolicy) compareBeginsEnds(peerBegins, peerEnds []gdplogd.HashAddr) ([]gdplogd.HashAddr, []gdplogd.HashAddr, []gdplogd.HashAddr, []gdplogd.HashAddr) {

}
