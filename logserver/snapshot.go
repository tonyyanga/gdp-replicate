package logserver

import (
	"github.com/tonyyanga/gdp-replicate/gdp"
)

// A SnapshotLogServer is a LogServer with snapshot capabilities
type SnapshotLogServer interface {
	SearchableLogServer

	CreateSnapshot() (*Snapshot, error)
	DestroySnapshot(*Snapshot)

	// Filter records by "time"
	// Implementations of this interface should determine how to
	// interpret "time" in snapshot and here
	CheckRecordExistence(time int, id gdp.Hash) (bool, error)
}

type Snapshot struct {
	time      int // Time of snapshot creation
	logServer SnapshotLogServer

	// newRecords are a map of hashes that are considered in the snapshot
	// although they are added to the db after its creation
	newRecords map[gdp.Hash]bool

	// logical starts and ends form the digest of the snapshot
	// see SimpleGraph for more details
	// logical starts map from PrevHash to their Hash TODO: better ideas?
	logicalStarts map[gdp.Hash][]gdp.Hash // value is PrevHash
	logicalEnds   map[gdp.Hash]bool
}

// MT: all methods of a Snapshot instance are not safe for multithreading
// The expectation is that at any time a snapshot is used by only one
// thread.

// save a record's hash in the snapshot to mark its existence and update
// the snapshot digest
func (s *Snapshot) SaveNewRecord(id gdp.Hash, prev gdp.Hash) {

}

func (s *Snapshot) GetLogicalStarts() map[gdp.Hash][]gdp.Hash {
	return s.logicalStarts
}

func (s *Snapshot) GetLogicalEnds() map[gdp.Hash]bool {
	return s.logicalEnds
}

// check the existence of a record hash in the snapshot
func (s *Snapshot) ExistRecord(id gdp.Hash) bool {
	exist, err := s.logServer.CheckRecordExistence(s.time, id)
	if err != nil {
		panic(err) // TODO
	}

	if exist {
		return true
	} else {
		_, ok := s.newRecords[id]
		return ok
	}
}

// search ahead & search after like utils in graph diff policy utils
// Return:
//   a list of hash addresses visited, not including start or terminals
//   a list of begins / ends in local graph reached
func (s *Snapshot) SearchAhead(start gdp.Hash, terminals []gdp.Hash) ([]gdp.Hash, []gdp.Hash) {
	terminalMap := gdp.InitSet(terminals)

	queryer := func(id gdp.Hash) (gdp.Hash, bool) {
		metadata, err := s.logServer.ReadMetadata([]gdp.Hash{id})
		if err != nil {
			panic(err) // TODO
		}

		if len(metadata) == 0 {
			return gdp.NullHash, false
		} else {
			return metadata[0].PrevHash, true
		}
	}

	return gdp.SearchAhead(start, terminalMap, queryer)
}

func (s *Snapshot) SearchAfter(start gdp.Hash, terminals []gdp.Hash) ([]gdp.Hash, []gdp.Hash) {
	terminalMap := gdp.InitSet(terminals)

	queryer := func(id gdp.Hash) ([]gdp.Hash, bool) {
		metadata, err := s.logServer.FindNextRecords(id)
		if err != nil {
			panic(err) // TODO
		}

		if len(metadata) == 0 {
			return nil, false
		} else {
			var result []gdp.Hash
			for _, m := range metadata {
				result = append(result, m.Hash)
			}

			return result, true
		}
	}

	return gdp.SearchAfter(start, terminalMap, queryer)
}
