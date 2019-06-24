package policy

import "github.com/tonyyanga/gdp-replicate/gdp"

type graphRepresentation interface {
	GetLogicalBegins() []gdp.Hash
	GetLogicalEnds() []gdp.Hash
}

type graphQueryable interface {
	SearchAhead(start gdp.Hash, terminals []gdp.Hash) ([]gdp.Hash, []gdp.Hash)
	SearchAfter(start gdp.Hash, terminals []gdp.Hash) ([]gdp.Hash, []gdp.Hash)
}

// Get peer policy context
func (policy *GraphDiffPolicy) getPeerPolicyContext(peer gdp.Hash) *peerPolicyContext {
	return &peerPolicyContext{
		graph:  policy.graphInUse[peer],
		policy: policy,
	}
}

// Return all connected hash addresses in the graph from a list of requested
// This function should handle deduplication
func (ctx *peerPolicyContext) getConnectedAddrs(addrs []gdp.Hash) []gdp.Hash {
	return getConnectedAddrs(ctx, addrs)
}

func getConnectedAddrs(graph graphQueryable, addrs []gdp.Hash) []gdp.Hash {
	empty := []gdp.Hash{}

	result := make(map[gdp.Hash]int)

	_getConnected := func(addr gdp.Hash) {
		// Add addr itself
		result[addr] = 1

		prev, _ := graph.SearchAhead(addr, empty)
		for _, node := range prev {
			result[node] = 1
		}

		next, _ := graph.SearchAfter(addr, empty)
		for _, node := range next {
			result[node] = 1
		}
	}

	for _, addr := range addrs {
		_getConnected(addr)
	}

	ret := []gdp.Hash{}
	for key := range result {
		ret = append(ret, key)
	}

	return ret
}

// Traverse ahead in the graph starting from "start". Traversal on a certain path ends when meeting a node in
// "terminals"
// Return:
//   a list of hash addresses visited, not including start or terminals
//   a list of begins / ends in local graph reached
func (ctx *peerPolicyContext) SearchAhead(start gdp.Hash, terminals []gdp.Hash) ([]gdp.Hash, []gdp.Hash) {
	actualMap := ctx.graph.GetActualPtrMap()
	terminalMap := gdp.InitSet(terminals)

	queryer := func(id gdp.Hash) (gdp.Hash, bool) {
		val, ok := actualMap[id]
		return val, ok
	}

	return gdp.SearchAhead(start, terminalMap, queryer)
}

// Traverse after in the graph starting from "start". Traversal on a certain path ends when meeting a node in
// "terminals"
// Return:
//   a list of hash addresses visited, not including start or terminals
//   a list of begins / ends in local graph reached
func (ctx *peerPolicyContext) SearchAfter(start gdp.Hash, terminals []gdp.Hash) ([]gdp.Hash, []gdp.Hash) {
	logicalMap := ctx.graph.GetLogicalPtrMap()

	queryer := func(id gdp.Hash) ([]gdp.Hash, bool) {
		val, ok := logicalMap[id]
		return val, ok
	}

	return gdp.SearchAfter(start, gdp.InitSet(terminals), queryer)
}

// Compare peer's begins and ends with my own.
// Return in the following order:
//   local begins not matched
//   local ends not matched
//   peer begins not matched
//   peer ends not matched
func (ctx *peerPolicyContext) compareBeginsEnds(
	peerBegins,
	peerEnds []gdp.Hash,
) ([]gdp.Hash, []gdp.Hash, []gdp.Hash, []gdp.Hash) {
	return compareGraphBeginsEnds(ctx.graph, peerBegins, peerEnds)
}

func compareGraphBeginsEnds(
	graph graphRepresentation,
	peerBegins,
	peerEnds []gdp.Hash,
) ([]gdp.Hash, []gdp.Hash, []gdp.Hash, []gdp.Hash) {
	localBegins := graph.GetLogicalBegins()
	localEnds := graph.GetLogicalEnds()
	localBeginsRet, peerBeginsRet := findDifferences(localBegins, peerBegins)
	localEndsRet, peerEndsRet := findDifferences(localEnds, peerEnds)
	return localBeginsRet, localEndsRet, peerBeginsRet, peerEndsRet
}
