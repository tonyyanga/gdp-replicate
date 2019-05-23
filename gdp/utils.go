package gdp

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

func (record *Record) MarshalBinary() (data []byte, err error) {
	return json.Marshal(record)
}

func (record *Record) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, &record)
}

func (hash Hash) Readable() string {
	return fmt.Sprintf("%X", hash)[:4]
}

func GenerateHash(seed string) Hash {
	return sha256.Sum256([]byte(seed))
}

// InitSet converts a HashAddr slice to a set
func InitSet(hashes []Hash) map[Hash]bool {
	set := make(map[Hash]bool)
	for _, hash := range hashes {
		set[hash] = false
	}
	return set
}

func SearchAhead(start Hash, terminals map[Hash]bool, queryFunc func(Hash) (Hash, bool)) ([]Hash, []Hash) {
	visited := make([]Hash, 0)
	localEnds := make([]Hash, 0)

	current := start
	prev, found := queryFunc(current)
	for found {
		if _, terminate := terminals[current]; terminate {
			// early termination because reaching terminal
			return visited, localEnds
		}

		current = prev
		prev, found = queryFunc(current)

		// Do not store the last pointer on the map
		if found {
			visited = append(visited, current)
		}
	}

	localEnds = append(localEnds, current)
	return visited, localEnds

}

func SearchAfter(start Hash, terminals map[Hash]bool, queryFunc func(Hash) ([]Hash, bool)) ([]Hash, []Hash) {
	// Use recursion since we may have branches, start is never included
	after, found := queryFunc(start)

	// base cases
	if _, terminate := terminals[start]; terminate {
		return []Hash{}, []Hash{}
	}

	if !found {
		return []Hash{}, []Hash{start}
	}

	visited := []Hash{}
	localEnds := make([]Hash, 0)
	for _, node := range after {
		visited_, localEnds_ := SearchAfter(node, terminals, queryFunc)
		visited = append(visited, node)
		visited = append(visited, visited_...)
		localEnds = append(localEnds, localEnds_...)
	}

	return visited, localEnds
}
