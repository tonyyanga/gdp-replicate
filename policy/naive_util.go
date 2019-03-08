package policy

import (
	"github.com/tonyyanga/gdp-replicate/gdp"
)

// getAllRecordHashes returns a slice of hashes in the graph
func (policy *NaivePolicy) getAllRecordHashes() []gdp.Hash {
	hashes := make([]gdp.Hash, 0, len(policy.logGraph.GetNodeMap()))
	for hash, _ := range policy.logGraph.GetNodeMap() {
		hashes = append(hashes, hash)
	}
	return hashes
}
