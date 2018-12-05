package daemon

import (
	"database/sql"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"github.com/tonyyanga/gdp-replicate/peers"
	"github.com/tonyyanga/gdp-replicate/policy"
)

type Daemon struct {
	httpAddr string
	myAddr   gdplogd.HashAddr
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
	myHashAddr gdplogd.HashAddr,
	peerAddrMap map[gdplogd.HashAddr]string,
) (Daemon, error) {
	db, err := sql.Open("sqlite3", sqlFile)

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
		myAddr:         myHashAddr,
		network:        peers.NewSimpleReplicateMgr(peerAddrMap),
		policy:         policy,
		conn:           conn,
		heartBeatState: 0,
		peerList:       peerList,
	}, nil
}

// Start begins listening for and sending heartbeats.
func (daemon Daemon) Start() error {
	go daemon.scheduleHeartBeat(2, daemon.fanOutHeartBeat(2))

	handler := func(src gdplogd.HashAddr, msg *policy.Message) {
		returnMsg := daemon.policy.ProcessMessage(msg, src)

		if returnMsg != nil {
			go daemon.network.Send(daemon.myAddr, src, returnMsg)
		}
	}

	err := daemon.network.ListenAndServe(daemon.httpAddr, handler)
	return err
}
