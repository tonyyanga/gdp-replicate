package gdplogd

type FakeGraph struct {
	logEntries    []LogEntryMetadata
	forwardEdges  HashAddrMultiMap
	backwardEdges map[HashAddr]HashAddr
	logicalEnds   []HashAddr
	logicalBegins []HashAddr
}

/*

InitFakeGraph creates a very simple graph that bypasses SQLite

Graph Diagram of FakeGraph
      3-5-6
     /
0-1-2     ...-7-8
     \
	  4

*/
func InitFakeGraph() (FakeGraph, error) {
	var fakeGraph FakeGraph

	logEntries := fakeGraph.logEntries
	for i := 0; i < 9; i++ {
		var hash HashAddr
		hash[i] = 1
		logEntries = append(logEntries, LogEntryMetadata{
			Hash:      hash,
			RecNo:     i,
			Timestamp: int64(i),
		})
	}
	logEntries[1].PrevHash = logEntries[0].Hash
	logEntries[2].PrevHash = logEntries[1].Hash
	logEntries[3].PrevHash = logEntries[2].Hash
	logEntries[4].PrevHash = logEntries[2].Hash
	logEntries[5].PrevHash = logEntries[3].Hash
	logEntries[6].PrevHash = logEntries[5].Hash

	var randomHash HashAddr
	randomHash[9] = 1

	logEntries[7].PrevHash = randomHash
	logEntries[8].PrevHash = logEntries[7].Hash

	fakeGraph.forwardEdges, fakeGraph.backwardEdges = getLogGraphs(logEntries)

	fakeGraph.logicalBegins = []HashAddr{
		logEntries[0].Hash,
		logEntries[7].Hash,
	}
	fakeGraph.logicalEnds = []HashAddr{
		logEntries[4].Hash,
		logEntries[6].Hash,
		logEntries[8].Hash,
	}

	return fakeGraph, nil
}

func (logGraph FakeGraph) GetActualPtrMap() map[HashAddr]HashAddr {
	return logGraph.backwardEdges
}

func (logGraph FakeGraph) GetLogicalPtrMap() HashAddrMultiMap {
	return logGraph.forwardEdges
}

func (logGraph FakeGraph) GetLogicalEnds() []HashAddr {
	return logGraph.logicalEnds
}

func (logGraph FakeGraph) GetLogicalBegins() []HashAddr {
	return logGraph.logicalBegins
}
