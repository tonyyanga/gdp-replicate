package gdplogd

import (
	"io"
)

// Represent a connection to a log daemon
type LogDaemonConnection interface {
    GetGraphs() (map[HashAddr]LogGraph, error)

    GetGraph(addr HashAddr) (*LogGraph, error)

    // TODO: It is unclear to me whether hash pointer are global across data capsules
    ReadLogMetadata(addr HashAddr) (*LogMetadata, error)
    ReadLogItem(addr HashAddr) (io.Reader, error)

    WriteLogItem(addr HashAddr, metadata *LogMetadata, data io.Reader) error

    ContainsLogItem(addr HashAddr) (bool, error)
}

