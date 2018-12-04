package daemon

import (
	"database/sql"
	"fmt"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"github.com/tonyyanga/gdp-replicate/peers"
	"github.com/tonyyanga/gdp-replicate/policy"
)

type Daemon struct {
	replicationNetworkManager peers.ReplicateNetworkMgr
	policy                    *policy.Policy
	conn                      gdplogd.LogDaemonConnection
}

// NewDaemon initializes Daemon for a log
func NewDaemon(sqlFile string, peerAddrMap map[gdplogd.HashAddr]string) (Daemon, error) {
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

	return Daemon{
		replicationNetworkManager: peers.NewSimpleReplicateMgr(peerAddrMap),
		conn:                      conn,
		policy:                    policy,
	}, nil
}

func (daemon Daemon) start() {
	daemon.replicationNetworkManager.ListenAndServe(":5000", msgPrinter)
}

func msgPrinter(msg *policy.Message) {
	fmt.Printf("received %s\n", *msg)
}
