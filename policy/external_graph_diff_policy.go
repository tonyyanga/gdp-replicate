package policy

import (
	"sync"

	"github.com/tonyyanga/gdp-replicate/logserver"
	"go.uber.org/zap"
)

// ExternalGraphDiffPolicy is a Policy with algorithms similar to GraphDiffPolicy
// However, ExternalGraphDiffPolicy does not require an in-memory copy of the
// metadata
type ExternalGraphDiffPolicy struct {
	logserver logserver.SnapshotLogServer

	// current snapshot in use for a specific peer
	// should reset to nil and be released when message exchange ends
	snapshotInUse map[gdp.Hash]logserver.Snapshot

	// last message sent to the peer
	// used to keep track of message exchanges state
	peerLastMsgType map[gdp.Hash]PeerState

	// mutex for each peer
	peerMutex map[gdp.Hash]*sync.Mutex
}

func NewExternalGraphDiffPolicy(logserver logserver.SnapshotLogServer) *ExternalGraphDiffPolicy {
	return &ExternalGraphDiffPolicy{
		logserver:       logserver,
		snapshotInUse:   make(map[gdp.Hash]logserver.Snapshot),
		peerLastMsgType: make(map[gdp.Hash]PeerState),
		peerMutex:       make(map[gdp.Hash]*sync.Mutex),
	}
}

func (policy *ExternalGraphDiffPolicy) GenerateMessage(dest gdp.Hash) (interface{}, error) {
	policy.initPeerIfNeeded(dest)

	policy.peerMutex[dest].Lock()
	defer policy.peerMutex[dest].Unlock()

	// update states to firstMsgSent
	snapshot, err := policy.logserver.CreateSnapshot()
	if err != nil {
		zap.S().Errorw(
			"Failed to create snapshot",
			"error", err,
		)
		return nil, err
	}

	policy.snapshotInUse[dest] = snapshot
	policy.peerLastMsgType[dest] = firstMsgSent

	// generate message
	content := &GraphMsgContent{
		Num:           first,
		LogicalBegins: policy.snapshotInUse[dest].GetLogicalBegins(),
		LogicalEnds:   policy.snapshotInUse[dest].GetLogicalEnds(),
	}

	zap.S().Infow("Generate first msg")
	return content, nil
}

func (policy *ExternalGraphDiffPolicy) ProcessMessage(src gdp.Hash, packedMsg interface{}) (
	interface{},
	error,
) {

}

func (policy *ExternalGraphDiffPolicy) initPeerIfNeeded(peer gdp.Hash) {
	mutex, ok := policy.peerMutex[peer]
	if !ok {
		policy.peerMutex[peer] = &sync.Mutex{}
		mutex = policy.peerMutex[peer]
	}

	mutex.Lock()
	defer mutex.Unlock()

	_, ok = policy.snapshotInUse[peer]
	if !ok {
		policy.snapshotInUse[peer] = nil
	}

	_, ok = policy.peerLastMsgType[peer]
	if !ok {
		policy.peerLastMsgType[peer] = noMsgExchanged
	}
}
