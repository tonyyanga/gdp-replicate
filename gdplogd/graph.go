/*
Package gdplogd supports interacting with gdplogd daemon which maintains
the source of truth on the log server
*/
package gdplogd

// A GDP graph supports query with Hash addresses
type HashAddr string

// Multimap for HashAddr
type HashAddrMultiMap map[HashAddr][]HashAddr

// Metadata for a log item
type LogMetadata struct {
    // TODO: I am not sure what are needed
    PrevPointer HashAddr
    Timestamp uint32
}

// LogGraph descriobe the graph of a DataCapsule on a log server
type LogGraph struct {
    // Address of the DataCapsule
    GraphAddr HashAddr

    // The actual hash pointer map, which follows:
    // A (oldest) <- B <- C (newest)
    HashPtrMap map[HashAddr]HashAddr
}

// LogGraphWrapper provides (and caches) typical usage of the Graph
type LogGraphWrapper interface {
    // The actual hash pointer map, which follows:
    // A (oldest) <- B <- C (newest)
    GetActualPtrMap() map[HashAddr]HashAddr

    // The logical hash pointer map, which follows:
    // A (oldest) -> B -> C (newest)
    GetLogicalPtrMap() HashAddrMultiMap

    // Nodes that have no entry in logical pointer map, e.g. C
    GetLogicalEnds() []HashAddr

    // Nodes that have dangling entries in the actual map
    // E.g. [X] <- D but there is no entry for X in the actual map; D has a dangling entry
    GetLogicalBegins() []HashAddr
}
