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
	CheckRecordExistence(time int64, id gdp.Hash) (bool, error)

	// Data store level implementation of searchs
	// If not implemented, return err
	SearchAhead(id gdp.Hash, time int64, newRecords []gdp.Hash, terminals []gdp.Hash) ([]gdp.Hash, []gdp.Hash, error)
	SearchAfter(id gdp.Hash, time int64, newRecords []gdp.Hash, terminals []gdp.Hash) ([]gdp.Hash, []gdp.Hash, error)
}

type Snapshot struct {
	time      int64 // Time of snapshot creation
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
func (s *Snapshot) RegisterNewRecord(id gdp.Hash, prev gdp.Hash) {
	s.newRecords[id] = true

	// If prev is not in the map, this record is a new logical start
	if !s.ExistRecord(prev) {
		s.logicalStarts[prev] = append(s.logicalStarts[id], prev)
	}

	// If no record has prevHash as id, this record is a new logical end
	metadata, err := s.logServer.FindNextRecords(id)
	if err != nil {
		panic(err) // TODO
	}

	if metadata == nil || len(metadata) == 0 {
		s.logicalEnds[id] = true
	}
}

func (s *Snapshot) RegisterNewRecords(records []gdp.Record) {
	for _, rec := range records {
		s.RegisterNewRecord(rec.Hash, rec.PrevHash)
	}
}

func (snapshot *Snapshot) GetLogicalEnds() []gdp.Hash {
	ends := make([]gdp.Hash, 0, len(snapshot.logicalEnds))
	for hash, _ := range snapshot.logicalEnds {
		ends = append(ends, hash)
	}
	return ends
}

func (snapshot *Snapshot) GetLogicalBegins() []gdp.Hash {
	starts := make([]gdp.Hash, 0, len(snapshot.logicalStarts))
	for _, hashes := range snapshot.logicalStarts {
		for _, hash := range hashes {
			starts = append(starts, hash)
		}
	}
	return starts
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
	visited, begins, err := s.logServer.SearchAhead(start, s.time, s.getNewRecords(), terminals)
	if err != nil {
		return s.searchAhead(start, terminals)
	} else {
		return visited, begins
	}
}

func (s *Snapshot) SearchAfter(start gdp.Hash, terminals []gdp.Hash) ([]gdp.Hash, []gdp.Hash) {
	visited, ends, err := s.logServer.SearchAfter(start, s.time, s.getNewRecords(), terminals)
	if err != nil {
		return s.searchAhead(start, terminals)
	} else {
		return visited, ends
	}
}

func (s *Snapshot) getNewRecords() []gdp.Hash {
    keys := make([]gdp.Hash, 0, len(s.newRecords))
    for k := range s.newRecords {
        keys = append(keys, k)
    }
    return keys
}

func (s *Snapshot) searchAhead(start gdp.Hash, terminals []gdp.Hash) ([]gdp.Hash, []gdp.Hash) {
	terminalMap := gdp.InitSet(terminals)

	queryer := func(id gdp.Hash) (gdp.Hash, bool) {
		metadata, err := s.logServer.ReadMetadata([]gdp.Hash{id})
		if err != nil {
			panic(err) // TODO
		}

		if metadata == nil || len(metadata) == 0 ||
			s.ExistRecord(metadata[0].Hash) {
			return gdp.NullHash, false
		} else {
			return metadata[0].PrevHash, true
		}
	}

	return gdp.SearchAhead(start, terminalMap, queryer)
}

func (s *Snapshot) searchAfter(start gdp.Hash, terminals []gdp.Hash) ([]gdp.Hash, []gdp.Hash) {
	terminalMap := gdp.InitSet(terminals)

	queryer := func(id gdp.Hash) ([]gdp.Hash, bool) {
		metadata, err := s.logServer.FindNextRecords(id)
		if err != nil {
			panic(err) // TODO
		}

		if metadata == nil || len(metadata) == 0 {
			return nil, false
		} else {
			var result []gdp.Hash
			for _, m := range metadata {
				if s.ExistRecord(m.Hash) {
					result = append(result, m.Hash)
				}
			}

			return result, true
		}
	}

	return gdp.SearchAfter(start, terminalMap, queryer)
}
