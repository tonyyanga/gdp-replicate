package gdplogd

// A GDP graph supports query with Hash addresses
type HashAddr string

// LogGraph descriobe the graph of a log server
type LogGraph interface {
    HaveLog(logAddr HashAddr) bool
    // TODO: more methods
}

// Represent a connection to a log daemon
type LogDaemonConnection interface {
    GetGraph() LogGraph
    // TODO: more methods
}
