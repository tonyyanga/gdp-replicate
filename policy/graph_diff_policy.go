package policy

import (
    "bufio"
    "bytes"
    "fmt"
    "sync"

    "github.com/tonyyanga/gdp-replicate/gdplogd"
)

// Message Types, four messages are required in chronological order
const (
    first = 0
    second
    third
    fourth
)

/*
Messages follow the following format

1. "begins\n" if applicable, followed by "<length>\n", then an array of hash addresses
2. "ends\n" if applicable, followed by "<length>\n", then an array of hash addresses
3. "requests\n" if applicable, followed by "<length>\n", then an array of hash addresses
4. "data\n" if applicable, followed by "<length>\n", then an array of hash addresses

TODO(tonyyanga): for data section, put actual data there in addition to hash addresses
*/

type PeerState int

// Peer states
const (
    noMsgExchanged = 0 // initial state

    firstMsgSent // initiator
    thirdMsgSent

    firstMsgRecved // receiver
    thirdMsgRecved
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
        policy.peerLastMsgType[peer] = noMsgExchanged
    }
}

func (policy *GraphDiffPolicy) resetPeerStatus(peer gdplogd.HashAddr) {
    policy.graphInUse[peer] = nil
    policy.peerLastMsgType[peer] = noMsgExchanged
}

func (policy *GraphDiffPolicy) GenerateMessage(dest gdplogd.HashAddr) *Message {
    // only create a message if the dest is at noMsgExchanged state
    // otherwise return nil
    policy.initPeerIfNeeded(dest)

    policy.peerMutex[dest].Lock()
    defer policy.peerMutex[dest].Unlock()

    if policy.peerLastMsgType[dest] != noMsgExchanged {
        return nil
    }

    // update states to firstMsgSent
    policy.graphInUse[dest] = policy.currentGraph
    policy.peerLastMsgType[dest] = firstMsgSent


    // generate message
    var buf bytes.Buffer
    buf.WriteString("begins\n")
    addrListToReader(policy.currentGraph.GetLogicalBegins(), &buf)

    buf.WriteString("ends\n")
    addrListToReader(policy.currentGraph.GetLogicalEnds(), &buf)

    return &Message{
        Type: first,
        Body: &buf,
    }
}

func (policy *GraphDiffPolicy) ProcessMessage(msg *Message, src gdplogd.HashAddr) *Message {
    policy.initPeerIfNeeded(src)

    policy.peerMutex[src].Lock()
    defer policy.peerMutex[src].Unlock()

    peerStatus := policy.peerLastMsgType[src]

    // validate peer status with incoming message
    // if status doesn't match the message type, simply reset the state machine
    if msg.Type == first {
        if peerStatus != noMsgExchanged {
            policy.resetPeerStatus(src)
            return nil
        }

        return policy.processFirstMsg(msg, src)
    } else if msg.Type == second {
        if peerStatus != firstMsgSent {
            policy.resetPeerStatus(src)
            return nil
        }

        return policy.processSecondMsg(msg, src)
    } else if msg.Type == third {
        if peerStatus != firstMsgRecved {
            policy.resetPeerStatus(src)
            return nil
        }

        return policy.processThirdMsg(msg, src)
    } else if msg.Type == fourth {
        if peerStatus != thirdMsgSent {
            policy.resetPeerStatus(src)
            return nil
        }

        return policy.processFourthMsg(msg, src)
    } else {
        panic(fmt.Sprintf("Reach unknown message type: %v", msg.Type))
    }

}

// Below are handlers for specific messages
// Handlers assume the mutex of the src is held by caller
func (policy *GraphDiffPolicy) processFirstMsg(msg *Message, src gdplogd.HashAddr) *Message {

}

func (policy *GraphDiffPolicy) processSecondMsg(msg *Message, src gdplogd.HashAddr) *Message {

}

func (policy *GraphDiffPolicy) processThirdMsg(msg *Message, src gdplogd.HashAddr) *Message {
    reader := bufio.NewReader(msg.Body)
    line, err := reader.ReadBytes('\n')

    if err != nil || bytes.Compare(line, []byte("requests\n")) != 0 {
        // Message is corrupted
        policy.resetPeerStatus(src)
        return nil
    }

    reqAddrs, err := addrListFromReader(reader)
    if err != nil {
        policy.resetPeerStatus(src)
        return nil
    }

    // For each addr requested, send the entire connected component
    addrs := policy.getConnectedAddrs(reqAddrs)

    retBody := &bytes.Buffer{}
    retBody.WriteString("data\n")
    err = policy.constructDataSection(addrs, retBody)
    if err != nil {
        policy.resetPeerStatus(src)
        return nil
    }

    ret := &Message{
        Type: fourth,
        Body: retBody,
    }

    line, err = reader.ReadBytes('\n')

    if err != nil || bytes.Compare(line, []byte("data\n")) != 0 {
        // Message is corrupted
        policy.resetPeerStatus(src)
        return nil
    }

    policy.processDataSection(reader)

    return ret
}

func (policy *GraphDiffPolicy) processFourthMsg(msg *Message, src gdplogd.HashAddr) *Message {
    reader := bufio.NewReader(msg.Body)
    line, err := reader.ReadBytes('\n')

    if err != nil || bytes.Compare(line, []byte("data\n")) != 0 {
        // Message is corrupted
        policy.resetPeerStatus(src)
        return nil
    }

    policy.processDataSection(reader)

    // last message, nothing to respond, reset state
    policy.resetPeerStatus(src)

    return nil
}
