package gdplogd

import (
	"io"
)

// Represent a connection to a log daemon
type LogDaemonConnection interface {
    GetGraphs() (map[string]LogGraph, error)

    GetGraph(name string) (*LogGraph, error)

    ReadLogMetadata(name string, addr HashAddr) (*LogMetadata, error)
    ReadLogItem(name string, addr HashAddr) (io.Reader, error)

    WriteLogItem(name string, addr HashAddr, metadata *LogMetadata, data io.Reader) error

    ContainsLogItem(name string, addr HashAddr) (bool, error)
}

