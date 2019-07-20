package policy

import (
	"sync"

	"github.com/tonyyanga/gdp-replicate/gdp"
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
	snapshotInUse map[gdp.Hash]*logserver.Snapshot

	// last message sent to the peer
	// used to keep track of message exchanges state
	peerLastMsgType map[gdp.Hash]PeerState

	// mutex for each peer
	peerMutex map[gdp.Hash]*sync.Mutex
}

func NewExternalGraphDiffPolicy(server logserver.SnapshotLogServer) *ExternalGraphDiffPolicy {
	return &ExternalGraphDiffPolicy{
		logserver:       server,
		snapshotInUse:   make(map[gdp.Hash]*logserver.Snapshot),
		peerLastMsgType: make(map[gdp.Hash]PeerState),
		peerMutex:       make(map[gdp.Hash]*sync.Mutex),
	}
}

func (policy *ExternalGraphDiffPolicy) getSnapshot(peer gdp.Hash) (*logserver.Snapshot, error) {
	snapshot, err := policy.logserver.CreateSnapshot()
	if err != nil {
		zap.S().Errorw(
			"Failed to create snapshot",
			"error", err,
		)
		policy.resetPeerStatus(peer)
		return nil, err
	}
	policy.snapshotInUse[peer] = snapshot
	return snapshot, nil
}

func (policy *ExternalGraphDiffPolicy) GenerateMessage(dest gdp.Hash) (interface{}, error) {
	policy.initPeerIfNeeded(dest)

	policy.peerMutex[dest].Lock()
	defer policy.peerMutex[dest].Unlock()

	if _, err := policy.getSnapshot(dest); err != nil {
		return nil, err
	}

	// update states to firstMsgSent
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
	zap.S().Debugw(
		"processing message",
		"src", src.Readable(),
	)

	msg, ok := packedMsg.(*GraphMsgContent)
	if !ok {
		return nil, errConversionError
	}
	policy.initPeerIfNeeded(src)

	policy.peerMutex[src].Lock()
	defer policy.peerMutex[src].Unlock()

	peerStatus := policy.peerLastMsgType[src]

	// validate peer status with incoming message
	// if status doesn't match the message type, simply reset the state machine
	switch msg.Num {
	case first:
		if peerStatus != noMsgExchanged {
			policy.resetPeerStatus(src)
			zap.S().Errorw(
				"inconsistent state and msg",
				"peerStatus", peerStatus,
				"msgNum", msg.Num,
			)
			return nil, errInconsistentStateAndMessage
		}

		return policy.processFirstMsg(msg, src)
	case second:
		if peerStatus != firstMsgSent {
			policy.resetPeerStatus(src)
			zap.S().Errorw(
				"inconsistent state and msg",
				"peerStatus", peerStatus,
				"msgNum", msg.Num,
			)
			return nil, errInconsistentStateAndMessage
		}

		return policy.processSecondMsg(msg, src)
	case third:
		if peerStatus != firstMsgRecved {
			policy.resetPeerStatus(src)
			zap.S().Errorw(
				"inconsistent state and msg",
				"peerStatus", peerStatus,
				"msgNum", msg.Num,
			)
			return nil, errInconsistentStateAndMessage
		}

		return policy.processThirdMsg(msg, src)
	case fourth:
		if peerStatus != thirdMsgSent {
			policy.resetPeerStatus(src)
			zap.S().Errorw(
				"inconsistent state and msg",
				"peerStatus", peerStatus,
				"msgNum", msg.Num,
			)
			return nil, errInconsistentStateAndMessage
		}

		return policy.processFourthMsg(msg, src)
	default:
		return nil, errUnknownMessageType
	}
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

// resetPeerState resets a peer's state to before any contact
// Assumes that the mutex of peer is held by caller
func (policy *ExternalGraphDiffPolicy) resetPeerStatus(peer gdp.Hash) {
	snapshot, ok := policy.snapshotInUse[peer]
	if ok && snapshot != nil {
		policy.logserver.DestroySnapshot(policy.snapshotInUse[peer])
	}
	policy.snapshotInUse[peer] = nil
	policy.peerLastMsgType[peer] = noMsgExchanged
}

// Below are handlers for specific messages
// Handlers assume the mutex of the src is held by caller
func (policy *ExternalGraphDiffPolicy) processFirstMsg(msg *GraphMsgContent, src gdp.Hash) (*GraphMsgContent, error) {
	var snapshot *logserver.Snapshot
	snapshot, err := policy.getSnapshot(src)
	if err != nil {
		return nil, err
	}

	policy.peerLastMsgType[src] = firstMsgRecved

	// Now that we have peer begins and ends, we start processing
	_, _, peerBeginsNotMatched, peerEndsNotMatched :=
		compareGraphBeginsEnds(snapshot, msg.LogicalBegins, msg.LogicalEnds)

	nodesToSend := make([]gdp.Hash, 0)

	// Send all nodes before (a node both of us have,
	// but that the peer thinks is a beginning)
	for _, begin := range peerBeginsNotMatched {
		if snapshot.ExistRecord(begin) {
			// Search all nodes ahead of begin to be sent to peer
			visited, _ := snapshot.SearchAhead(begin, msg.LogicalEnds)
			nodesToSend = append(nodesToSend, visited...)
		}
	}

	// Send all nodes after (a node that both of us have,
	// but that the peer thinks is an end)
	for _, end := range peerEndsNotMatched {
		if snapshot.ExistRecord(end) {
			// Search all nodes after end to be sent to peer
			visited, _ := snapshot.SearchAfter(end, msg.LogicalBegins)
			nodesToSend = append(nodesToSend, visited...)
		}
	}

	recordsNotInRX, err := policy.logserver.ReadRecords(nodesToSend)
	if err != nil {
		policy.resetPeerStatus(src)
		return nil, err
	}

	msgContent := &GraphMsgContent{
		Num:            second,
		RecordsNotInRX: recordsNotInRX,
		LogicalBegins:  snapshot.GetLogicalBegins(),
		LogicalEnds:    snapshot.GetLogicalEnds(),
	}

	policy.peerLastMsgType[src] = firstMsgRecved
	zap.S().Infow(
		"Generating second message",
		"numRecords", len(msgContent.RecordsNotInRX),
		"numBegins", len(msgContent.LogicalBegins),
		"numEnds", len(msgContent.LogicalEnds),
	)

	return msgContent, nil
}

func (policy *ExternalGraphDiffPolicy) processSecondMsg(msg *GraphMsgContent, src gdp.Hash) (*GraphMsgContent, error) {
	var snapshot *logserver.Snapshot
	snapshot, err := policy.getSnapshot(src)
	if err != nil {
		return nil, err
	}

	myBeginsNotMatched,
		myEndsNotMatched,
		peerBeginsNotMatched,
		peerEndsNotMatched :=
		compareGraphBeginsEnds(snapshot, msg.LogicalBegins, msg.LogicalEnds)

	nodesToSend := make([]gdp.Hash, 0)
	componentsToSend := make([]gdp.Hash, 0)
	requests := make([]gdp.Hash, 0)

	myBeginsEndsToSend := make(map[gdp.Hash]int)

	for _, begin := range peerBeginsNotMatched {
		if snapshot.ExistRecord(begin) {
			// Search all nodes ahead of begin to be sent to peer
			// If we reach a begin / end of local graph, add to myBeginsEndsToSend
			visited, localEnds := snapshot.SearchAhead(begin, msg.LogicalEnds)
			nodesToSend = append(nodesToSend, visited...)

			for _, node := range localEnds {
				myBeginsEndsToSend[node] = 1
			}
		} else {
			// Add the entire connected component to request
			requests = append(requests, begin)
		}
	}

	for _, end := range peerEndsNotMatched {
		if snapshot.ExistRecord(end) {
			// Search all nodes ahead of begin to be sent to peer
			// If we reach a begin / end of local graph, add to myBeginsEndsToSend
			visited, localEnds := snapshot.SearchAfter(end, msg.LogicalBegins)
			nodesToSend = append(nodesToSend, visited...)

			for _, node := range localEnds {
				myBeginsEndsToSend[node] = 1
			}
		} else {
			// Add the entire connected component to request
			requests = append(requests, end)
		}
	}

	for _, begin := range myBeginsNotMatched {
		if _, found := myBeginsEndsToSend[begin]; !found {
			// Add the connected component to nodesToSend
			componentsToSend = append(componentsToSend, begin)
		}
	}

	for _, end := range myEndsNotMatched {
		if _, found := myBeginsEndsToSend[end]; !found {
			// Add the connected component to nodesToSend
			componentsToSend = append(componentsToSend, end)
		}
	}

	componentsToSend = getConnectedAddrs(snapshot, componentsToSend)
	nodesToSend = append(nodesToSend, componentsToSend...)
	recordsToSend, err := policy.logserver.ReadRecords(nodesToSend)
	if err != nil {
		policy.resetPeerStatus(src)
		return nil, err
	}

	resp := &GraphMsgContent{
		Num:            third,
		HashesTXWants:  requests,
		RecordsNotInRX: recordsToSend,
	}

	zap.S().Infow(
		"Generating message third",
	)

	policy.peerLastMsgType[src] = thirdMsgSent
	return resp, nil
}

func (policy *ExternalGraphDiffPolicy) processThirdMsg(msg *GraphMsgContent, src gdp.Hash) (*GraphMsgContent, error) {
	var snapshot *logserver.Snapshot
	snapshot, err := policy.getSnapshot(src)
	if err != nil {
		return nil, err
	}

	snapshot.RegisterNewRecords(msg.RecordsNotInRX)
	err = policy.logserver.WriteRecords(msg.RecordsNotInRX)
	if err != nil {
		policy.resetPeerStatus(src)
		return nil, err
	}

	reqAddrs := msg.HashesTXWants

	// For each addr requested, send the entire connected component
	addrs := getConnectedAddrs(snapshot, reqAddrs)
	recordsRXWants, err := policy.logserver.ReadRecords(addrs)
	if err != nil {
		policy.resetPeerStatus(src)
		return nil, err
	}

	resp := &GraphMsgContent{
		Num:            fourth,
		RecordsNotInRX: recordsRXWants,
	}

	zap.S().Infow(
		"Generating fourth message",
		"numRecords", len(recordsRXWants),
	)

	// When to revert to netural state?
	policy.peerLastMsgType[src] = thirdMsgRecved

	return resp, nil
}

func (policy *ExternalGraphDiffPolicy) processFourthMsg(msg *GraphMsgContent, src gdp.Hash) (*GraphMsgContent, error) {
	err := policy.logserver.WriteRecords(msg.RecordsNotInRX)
	if err != nil {
		policy.resetPeerStatus(src)
		return nil, err
	}

	// last message, nothing to respond, reset state
	policy.resetPeerStatus(src)
	return nil, ErrConversationFinished
}
