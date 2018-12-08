package policy

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"go.uber.org/zap"
)

// PeerStates

const (
	resting          = 0
	initHeartBeat    = 1
	receiveHeartBeat = 2
)

type NaivePolicy struct {
	db      *sql.DB
	name    string
	myState map[gdplogd.HashAddr]PeerState
}

// Unused for NaivePolicy
func (policy *NaivePolicy) getLogDaemonConnection() gdplogd.LogDaemonConnection {
	return nil
}

// Unused for NaivePolicy
func (policy *NaivePolicy) UpdateCurrGraph() error {
	return nil
}

func (policy *NaivePolicy) GenerateMessage(dest gdplogd.HashAddr) *Message {
	policy.initPeerIfNeeded(dest)
	var buf bytes.Buffer

	// Write every hash into message
	hashes, err := GetAllLogHashes(policy.db)
	if err != nil {
		zap.S().Errorw(
			"Failed to read all log hashes from db",
			"error", err.Error(),
		)
	}
	addrListToReader(hashes, &buf)

	// change my state to initHeartBeat
	policy.myState[dest] = initHeartBeat

	return &Message{
		Type: first,
		Body: &buf,
	}
}
func (policy *NaivePolicy) ProcessMessage(
	msg *Message,
	src gdplogd.HashAddr,
) *Message {
	zap.S().Debugw(
		"processing message",
		"msg", msg,
		"src", gdplogd.ReadableAddr(src),
	)
	policy.initPeerIfNeeded(src)

	myState := policy.myState[src]

	if msg.Type == first && myState == resting {
		return policy.processFirstMsg(msg, src)
	} else if msg.Type == second && myState == initHeartBeat {
		return policy.processSecondMsg(msg, src)
	} else if msg.Type == third && myState == receiveHeartBeat {
		return policy.processThirdMsg(msg, src)
	} else {
		policy.myState[src] = resting
		return nil
	}
}

func (policy *NaivePolicy) processFirstMsg(msg *Message, src gdplogd.HashAddr) *Message {
	// read all hashes
	reader := bufio.NewReader(msg.Body)
	theirHashes, err := addrListFromReader(reader)
	if err != nil {
		zap.S().Errorw(
			"Error parsing msg",
			"error", err.Error(),
		)
	}

	// compute my hashes
	myHashes, err := GetAllLogHashes(policy.db)
	if err != nil {
		zap.S().Errorw(
			"Failed to read all log hashes from db",
			"error", err.Error(),
		)
	}
	fmt.Printf("%s %s", theirHashes, myHashes)

	// find the differences
	onlyMine, onlyTheirs := findDifferences(myHashes, theirHashes)

	// load the logs with hashes that only I had
	onlyMyLogs, err := GetLogEntries(policy.db, myHashes)

	var buf bytes.Buffer
	msgToSend := &Message{
		Type: second,
		Body: &buf,
	}

	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(onlyMyLogs); err != nil {
		zap.S().Errorw(
			"Error encoding log entries",
			"error", err.Error(),
		)
	}

	// send data, requests

	return msgToSend
}

func (policy *NaivePolicy) processSecondMsg(msg *Message, src gdplogd.HashAddr) *Message {
	// identify requests

	// load requests

	// save received data

	// send data for requests
	return nil
}

func (policy *NaivePolicy) processThirdMsg(msg *Message, src gdplogd.HashAddr) *Message {
	// save receivved data
	return nil
}

func NewNaivePolicy(db *sql.DB, name string) *NaivePolicy {
	return &NaivePolicy{
		db:      db,
		name:    name,
		myState: make(map[gdplogd.HashAddr]PeerState),
	}
}

func (policy *NaivePolicy) initPeerIfNeeded(peer gdplogd.HashAddr) {
	_, present := policy.myState[peer]
	if !present {
		policy.myState[peer] = resting
	}
}
