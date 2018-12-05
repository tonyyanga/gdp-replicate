package daemon

import (
	"math/rand"
	"time"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
)

// Send a heart beat every INTERVAL seconds
func (daemon Daemon) scheduleHeartBeat(interval int) error {
	ticker := time.NewTicker(time.Second * 10)
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
		return nil
	}

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
