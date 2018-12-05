package policy

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"strconv"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
)

// Get peer policy context
func (policy *GraphDiffPolicy) getPeerPolicyContext(peer gdplogd.HashAddr) *peerPolicyContext {
	return &peerPolicyContext{
		graph:  policy.graphInUse[peer],
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
func (ctx *peerPolicyContext) constructDataSection(addrs []gdplogd.HashAddr, dest *bytes.Buffer) error {
	// First, write how many items to expect
	dest.WriteString(strconv.Itoa(len(addrs)))
	dest.WriteString("\n")
	for _, addr := range addrs {
		dataReader, err := ctx.policy.conn.ReadLogItem(ctx.policy.name, addr)
		if err != nil {
			return err
		}

		metadata, err := ctx.policy.conn.ReadLogMetadata(ctx.policy.name, addr)
		if err != nil {
			return err
		}

		// Timestamp and other Metadata should be included TODO

		var data bytes.Buffer
		_, err = data.ReadFrom(dataReader)
		if err != nil {
			return err
		}

		// First, length of the data portion
		dest.WriteString(strconv.Itoa(data.Len()))
		dest.WriteString("\n")

		// Second, write the metadata: 32 byte address + 32 bytes prev pointer
		dest.Write(addr[:])
		dest.Write(metadata.PrevHash[:])

		// Third, write actual data
		dest.ReadFrom(&data)
	}

	return nil
}

// Process data section of the message and update the current graph accordingly
// Assume that "data\n" is already consumed
func (ctx *peerPolicyContext) processDataSection(body io.Reader) {
	// TODO: proper error reporting should be in place

	reader := bufio.NewReader(body)

	length_, err := reader.ReadBytes('\n')
	if err != nil {
		log.Printf("Error while reading: %v", err)
		return
	}
	length_ = length_[:len(length_)-1]

	length, err := strconv.Atoi(string(length_))
	if err != nil {
		log.Printf("%v", err)
		return
	}

	updates := make([]gdplogd.LogEntryMetadata, 0)

	for ; length > 0; length-- {
		// Read an individual block
		dataLength_, err := reader.ReadBytes('\n')
		if err != nil {
			log.Printf("%v", err)
			return
		}
		dataLength_ = dataLength_[:len(dataLength_)-1]

		dataLength, err := strconv.Atoi(string(dataLength_))
		if err != nil {
			log.Printf("%v", err)
			return
		}

		var addr gdplogd.HashAddr
		var prev gdplogd.HashAddr
		_, err = io.ReadFull(reader, addr[:])
		if err != nil {
			log.Printf("%v", err)
			return
		}

		_, err = io.ReadFull(reader, prev[:])
		if err != nil {
			log.Printf("%v", err)
			return
		}

		data := make([]byte, dataLength)
		_, err = io.ReadFull(reader, data)
		if err != nil {
			log.Printf("%v", err)
			return
		}

		metadata := gdplogd.LogEntryMetadata{
			Hash:     addr,
			PrevHash: prev,
			// TODO
		}

		ctx.tryStoreData(metadata, data)

		updates = append(updates, metadata)
	}

	if ctx.graph == nil {
		panic("")
	}
	ctx.graph.AcceptNewLogEntries(updates)
}

// Try to store the data at addr
// Return whether the data is stored via this call, or already in gdplogd
func (ctx *peerPolicyContext) tryStoreData(metadata gdplogd.LogEntryMetadata, data []byte) bool {
	conn := ctx.policy.conn
	name := ctx.policy.name
	addr := metadata.Hash

	// Update the graph
	// TODO: call refresh graph interface

	// Update the connection
	contains, err := conn.ContainsLogItem(name, addr)
	if err != nil {
		return false
	}

	if contains {
		return false
	} else {
		// TODO: proper error handling
		conn.WriteLogItem(name, &metadata, bytes.NewBuffer(data))
		return true
	}

}

// Traverse ahead in the graph starting from "start". Traversal on a certain path ends when meeting a node in
// "terminals"
// Return:
//   a list of hash addresses visited, not including start or terminals
//   a list of begins / ends in local graph reached
func (ctx *peerPolicyContext) searchAhead(start gdplogd.HashAddr, terminals []gdplogd.HashAddr) ([]gdplogd.HashAddr, []gdplogd.HashAddr) {
	actualMap := ctx.graph.GetActualPtrMap()
	terminalMap := addrSliceToMap(terminals)

	visited := make([]gdplogd.HashAddr, 0)
	localEnds := make([]gdplogd.HashAddr, 0)

	current := start
	prev, found := actualMap[current]
	for found {
		if _, terminate := terminalMap[current]; terminate {
			// early termination because reaching terminal
			return visited, localEnds
		}

		current = prev
		prev, found = actualMap[current]

		// Do not store the last pointer on the map
		if found {
			visited = append(visited, current)
		}
	}

	localEnds = append(localEnds, current)
	return visited, localEnds
}

// Traverse after in the graph starting from "start". Traversal on a certain path ends when meeting a node in
// "terminals"
// Return:
//   a list of hash addresses visited, not including start or terminals
//   a list of begins / ends in local graph reached
func (ctx *peerPolicyContext) searchAfter(start gdplogd.HashAddr, terminals []gdplogd.HashAddr) ([]gdplogd.HashAddr, []gdplogd.HashAddr) {
	return ctx._searchAfter(start, addrSliceToMap(terminals))
}

func (ctx *peerPolicyContext) _searchAfter(start gdplogd.HashAddr, terminals map[gdplogd.HashAddr]int) ([]gdplogd.HashAddr, []gdplogd.HashAddr) {
	logicalMap := ctx.graph.GetLogicalPtrMap()

	// Use recursion since we may have branches, start is never included
	after, found := logicalMap[start]

	// base cases
	if _, terminate := terminals[start]; terminate {
		return []gdplogd.HashAddr{}, []gdplogd.HashAddr{}
	}

	if !found {
		return []gdplogd.HashAddr{}, []gdplogd.HashAddr{start}
	}

	visited := []gdplogd.HashAddr{}
	localEnds := make([]gdplogd.HashAddr, 0)
	for _, node := range after {
		visited_, localEnds_ := ctx._searchAfter(node, terminals)
		visited = append(visited, node)
		visited = append(visited, visited_...)
		localEnds = append(localEnds, localEnds_...)
	}

	return visited, localEnds
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
		localMap := addrSliceToMap(local)
		peerMap := addrSliceToMap(peer)

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
