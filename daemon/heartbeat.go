package daemon

import (
	"math/rand"
	"time"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"go.uber.org/zap"
)

// Send a heart beat every INTERVAL seconds
func (daemon Daemon) scheduleHeartBeat(interval int) error {
	zap.S().Infow(
		"scheduling heartbeat",
		"interval", interval,
	)
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	for _ = range ticker.C {
		err := daemon.cycleHeartBeat()
		if err != nil {
			return err
		}
	}
	return nil
}

// Sends a heartbeat message to PEER if necessary
func (daemon Daemon) sendHeartBeat(peer gdplogd.HashAddr) error {
	msg := daemon.policy.GenerateMessage(peer)

	// A msg may not need to be sent
	if msg == nil {
		zap.S().Infow(
			"no heartbeat sent",
			"src", daemon.myAddr,
			"dst", peer,
		)
		return nil
	}

	zap.S().Infow(
		"heart beat sent",
		"src", daemon.myAddr,
		"dst", peer,
		"msg", msg,
	)
	return daemon.network.Send(daemon.myAddr, peer, msg)
}

// Send a heartbeat message to one of daemon peers.
// Cycles through each of the peers
func (daemon Daemon) cycleHeartBeat() error {
	peerIndex := daemon.heartBeatState % len(daemon.peerList)
	peer := daemon.peerList[peerIndex]
	daemon.heartBeatState += 1
	return daemon.sendHeartBeat(peer)
}

// Send a heartbeat message to one of daemon peers.
// Randomly selects a peer with repetition
func (daemon Daemon) randomHeartBeat() error {
	peerIndex := rand.Intn(len(daemon.peerList))
	peer := daemon.peerList[peerIndex]
	return daemon.sendHeartBeat(peer)
}
