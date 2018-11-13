/*
Package policy provides different policies used to

1) generate heartbeat messages that represent the state of gdplogd
2) process heartbeat messages to find out required updates for gdplogd

A policy only deals with metadata, i.e. which piece of data is missing,
if any. It does not actually deal with transmission of the data.
*/

package policy

import (
    "io"

    "github.com/tonyyanga/gdp-replicate/gdplogd"
)

type MessageType int

const (
    // Request & Response for Metadata
    ReqMeta MessageType = 1
    RespMeta

    // Request & Response for Data
    ReqData
    RespData
)

// Message is a data structure to talk to peers
type Message struct {
    Type MessageType
    Body io.Reader
}

// Interface for a Policy that deals with Messages
type Policy interface {
    // Generate message from a graph
    GenerateMessage(graph *gdplogd.LogGraph) *Message

    // Process a message and return an array of missing data
    ProcessMessage(msg *Message) []gdplogd.HashAddr
}
