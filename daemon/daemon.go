package daemon

import (
	"database/sql"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"github.com/tonyyanga/gdp-replicate/peers"
	"github.com/tonyyanga/gdp-replicate/policy"
)

type Daemon struct {
	httpAddr string
	network  peers.ReplicateNetworkMgr
	policy   policy.Policy
	conn     gdplogd.LogDaemonConnection
	// Controls the randomness of sending heart beats to peers
	heartBeatState int
	peerList       []gdplogd.HashAddr
}

// NewDaemon initializes Daemon for a log
func NewDaemon(
	httpAddr,
	sqlFile string,
	peerAddrMap map[gdplogd.HashAddr]string,
) (Daemon, error) {
	db, err := sql.Open("sqlite3", sqlFile)
	var graphAddr gdplogd.HashAddr

	conn, err := gdplogd.InitLogDaemonConnector(db, "default")
	if err != nil {
		return Daemon{}, err
	}

	graph, err := conn.GetGraph("default")
	if err != nil {
		return Daemon{}, err
	}
	policy := policy.NewGraphDiffPolicy(conn, "policy-name", *graph)

	// Create list of peers
	peerList := make([]gdplogd.HashAddr, len(peerAddrMap))
	for peer := range peerAddrMap {
		peerList = append(peerList, peer)
	}

	return Daemon{
		httpAddr:       httpAddr,
		network:        peers.NewSimpleReplicateMgr(peerAddrMap),
		policy:         policy,
		conn:           conn,
		heartBeatState: 0,
		peerList:       peerList,
	}, nil
}

// Start begins listening for and sending heartbeats.
func (daemon Daemon) start() {
	go daemon.network.ListenAndServe(daemon.httpAddr, msgPrinter)
	go daemon.scheduleHeartBeat(2)
}
