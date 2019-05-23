package policy

import "github.com/tonyyanga/gdp-replicate/gdp"

type PeerState int

// Message Types
const (
	first int = iota
	second
	third
	fourth
)

// findDifferences determines which hashes are exclusive to only one list.
// e.g. finding the non-union parts of a Venn diagram
func findDifferences(
	myHashes,
	theirHashes []gdp.Hash,
) (onlyMine []gdp.Hash, onlyTheirs []gdp.Hash) {
	mySet := gdp.InitSet(myHashes)
	theirSet := gdp.InitSet(theirHashes)

	for myHash := range mySet {
		_, present := theirSet[myHash]
		if !present {
			onlyMine = append(onlyMine, myHash)
		}
	}
	for theirHash := range theirSet {
		_, present := mySet[theirHash]
		if !present {
			onlyTheirs = append(onlyTheirs, theirHash)
		}
	}

	return onlyMine, onlyTheirs
}
