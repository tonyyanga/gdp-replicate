package policy

import (
    "bytes"
    "sync"

    "github.com/tonyyanga/gdp-replicate/gdplogd"
)

// Message Types, four messages are required
const (
    first = 0
    second
    third
    fourth
)

type PeerState int

// Peer states
const (
    noMsgSent = 0
    firstMsgSent
    secondMsgSent
)

// GraphDiffPolicy implements the Policy interface and uses diff of
// begins and ends of graph to detect differences
// See algorithm spec on Dropbox Paper for more details
// TODO(tonyyanga): move the algorithm here
type GraphDiffPolicy struct {
    conn *gdplogd.LogDaemonConnection // connection

    currentGraph gdplogd.LogGraphWrapper // most up to date graph

    // current graph in use for a specific peer
    // should reset to nil when message exchange ends
    graphInUse map[gdplogd.HashAddr]gdplogd.LogGraphWrapper

    // last message sent to the peer
    // used to keep track of message exchanges state
    peerLastMsgType map[gdplogd.HashAddr]PeerState

    // mutex for each peer
    peerMutex map[gdplogd.HashAddr]*sync.Mutex
}

// Constructor with log daemon connection and initial graph
func NewGraphDiffPolicy(conn *gdplogd.LogDaemonConnection, graph gdplogd.LogGraphWrapper) *GraphDiffPolicy {
    return &GraphDiffPolicy{
        conn: conn,
        currentGraph: graph,
        graphInUse: make(map[gdplogd.HashAddr]gdplogd.LogGraphWrapper),
        peerLastMsgType: make(map[gdplogd.HashAddr]PeerState),
        peerMutex: make(map[gdplogd.HashAddr]*sync.Mutex),
    }
}

func (policy *GraphDiffPolicy) getLogDaemonConnection() *gdplogd.LogDaemonConnection {
    return policy.conn
}

func (policy *GraphDiffPolicy) AcceptNewGraph(graph gdplogd.LogGraphWrapper) {
    policy.currentGraph = graph
}

func (policy *GraphDiffPolicy) initPeerIfNeeded(peer gdplogd.HashAddr) {
    mutex, ok := policy.peerMutex[peer]
    if !ok {
        policy.peerMutex[peer] = &sync.Mutex{}
    }

    mutex.Lock()
    defer mutex.Unlock()

    _, ok = policy.graphInUse[peer]
    if !ok {
        policy.graphInUse[peer] = nil
    }

    _, ok = policy.peerLastMsgType[peer]
    if !ok {
        policy.peerLastMsgType[peer] = noMsgSent
    }
}

func (policy *GraphDiffPolicy) GenerateMessage(dest gdplogd.HashAddr) *Message {
    // only create a message if the dest is at noMsgSent state
    // otherwise return nil
    policy.initPeerIfNeeded(dest)

    policy.peerMutex[dest].Lock()
    defer policy.peerMutex[dest].Unlock()

    if policy.peerLastMsgType[dest] != noMsgSent {
        return nil
    }

    // update states to firstMsgSent
    policy.graphInUse[dest] = policy.currentGraph
    policy.peerLastMsgType[dest] = firstMsgSent

    // generate message
    var buf bytes.Buffer
    buf.WriteString("begins")
    buf.WriteString("\n")
    addrListToReader(policy.currentGraph.GetLogicalBegins(), &buf)

    buf.WriteString("ends")
    buf.WriteString("\n")
    addrListToReader(policy.currentGraph.GetLogicalEnds(), &buf)

    return &Message{
        Type: first,
        Body: &buf,
    }
}

func (policy *GraphDiffPolicy) ProcessMessage(msg *Message, src gdplogd.HashAddr) *Message {
    // TODO
    return nil
}
