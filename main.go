package main

// #include <stdint.h>
// #include "gdp_types.h"
// #include "gdp_helper.h"
import "C"

import (
	"log"

	"github.com/tonyyanga/gdp-replicate/daemon"
	"github.com/tonyyanga/gdp-replicate/gdp"
	"go.uber.org/zap"
)

/* CreateLogSyncHandle creates the context for a log in the log server.
   The returned LogSyncHandle manages the global sync status of this log. */
//export CreateLogSyncHandle
func CreateLogSyncHandle(sqlFile string) (C.LogSyncHandle, C.int) {
	ticket, err := newLogSyncCtx(sqlFile)
	if err != nil {
		log.Printf("%v", err)
		return C.LogSyncHandle{handleTicket: 0}, 1
	}

	zap.S().Infow(
		"Created new log sync handle",
		"sql-file", sqlFile,
		"ticket", ticket,
	)

	cTicket := *(*C.uint32_t)(&ticket)

	return C.LogSyncHandle{handleTicket: cTicket}, 0
}

/* Call ReleaseLogSyncHandle to release corresponding memory in Go */
//export ReleaseLogSyncHandle
func ReleaseLogSyncHandle(handle C.LogSyncHandle) {
	delete(logCtxMap, uint32(handle.handleTicket))
}

/* Trigger a sync with a given peer */
//export InitSync
func InitSync(handle C.LogSyncHandle, peer C.PeerAddr) C.Msg {
	ctx, err := getLogSyncCtx(handle)
	if err != nil {
		// TODO
		return C.Msg{}
	}

	gdpAddr := peerAddrToHash(peer)

	policy := ctx.Policy
	msg, err := policy.GenerateMessage(gdpAddr)

	return toCMsg(msg)
}

/* Provide an incoming message to the library to process
   This function releases data in msg. */
//export HandleMsg
func HandleMsg(handle C.LogSyncHandle, peer C.PeerAddr, msg C.Msg) C.Msg {
	ctx, err := getLogSyncCtx(handle)
	if err != nil {
		// TODO
		return C.Msg{}
	}

	gdpAddr := peerAddrToHash(peer)

	policy := ctx.Policy
	respMsg, err := policy.ProcessMessage(gdpAddr, toGoMsg(msg))

	return toCMsg(respMsg)
}

// empty main func required to compile to a shared library
func main() {
	selfGDPAddr := gdp.GenerateHash("some identifier")
	daemon.InitLogger(selfGDPAddr)
}
