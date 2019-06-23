package main

// #include <stdint.h>
// #include "gdp_types.h"
// #include "gdp_helper.h"
import "C"

import (
	"log"

	"github.com/tonyyanga/gdp-replicate/daemon"
	"github.com/tonyyanga/gdp-replicate/gdp"
	"github.com/tonyyanga/gdp-replicate/policy"
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

/* Trigger a sync with a given peer

   Memory convention:
   Memory pointed to by the returned C.Msg data pointer should
   be freed by the caller.
*/
//export InitSync
func InitSync(handle C.LogSyncHandle, peer C.PeerAddr) (C.Msg, C.int) {
	ctx, err := getLogSyncCtx(handle)
	if err != nil {
		// TODO
		return C.Msg{}, 1
	}

	gdpAddr := peerAddrToHash(peer)

	policy := ctx.Policy
	msg, err := policy.GenerateMessage(gdpAddr)
	if err != nil {
		// TODO
		return C.Msg{}, 1
	}

	return toCMsg(msg), 0
}

/* Provide an incoming message to the library to process
   This function releases data in msg.

   Return code:
   0 = successful completion
   1 = an error occurred, C.Msg should not be used
   -1 = no message returned, C.Msg should not be used

   Memory convention:
   Memory pointed to by the pointer in the C.Msg provided is
   managed by the caller. This function does not free it.
   Memory pointed to by the returned C.Msg data pointer should
   be freed by the caller.
*/
//export HandleMsg
func HandleMsg(handle C.LogSyncHandle, peer C.PeerAddr, msg C.Msg) (C.Msg, C.int) {
	completionErr := policy.ErrConversationFinished

	ctx, err := getLogSyncCtx(handle)
	if err != nil {
		// TODO
		return C.Msg{}, 1
	}

	gdpAddr := peerAddrToHash(peer)

	policy := ctx.Policy
	respMsg, err := policy.ProcessMessage(gdpAddr, toGoMsg(msg))
	if err != nil {
		if err == completionErr {
			// Policy completed
			return C.Msg{}, -1
		} else {
			// TODO
			return C.Msg{}, 1
		}
	}

	return toCMsg(respMsg), 0
}

// empty main func required to compile to a shared library
func main() {
	// TODO: I think main should be empty. Where do I initialize?
	selfGDPAddr := gdp.GenerateHash("some identifier")
	daemon.InitLogger(selfGDPAddr)
}
