/*
Package policy provides different policies used to

1) generate heartbeat messages that represent the state of gdplogd
2) process heartbeat messages to find out required updates for gdplogd

A policy only deals with metadata, i.e. which piece of data is missing,
if any. It does not actually deal with transmission of the data.
*/

package policy

import "github.com/tonyyanga/gdp-replicate/gdplogd"

type MessageType int

const (
    Request MessageType = 1
    Response MessageType = 2
)

type Message interface {
    // A message must have a type
    GetMessageType() MessageType
}

type ResponseMessage interface {
    Message

    GetMissingData() []gdplogd.HashAddr
}

type Policy interface {
    // Generate message from a graph
    GenerateMessage(graph gdplogd.LogGraph) Message

    // Process a message and return an array of missing data
    ProcessMessage(msg Message) []gdplogd.HashAddr
}
