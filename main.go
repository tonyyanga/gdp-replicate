package main

// #include "gdp_types.h"
// #include "gdp_helper.h"
import "C"

/* CreateLogSyncHandle creates the context for a log in the log server.
   The returned LogSyncHandle manages the global sync status of this log. */
//export CreateLogSyncHandle
func CreateLogSyncHandle(sqlFile string, callback C.MsgCallbackFunc) C.LogSyncHandle {
	// TODO
	return C.LogSyncHandle{}
}

/* Call ReleaseLogSyncHandle to release corresponding memory in Go */
//export ReleaseLogSyncHandle
func ReleaseLogSyncHandle(handle C.LogSyncHandle) {
	// TODO
}

/* Synchronization messages will be passed to the user via the MsgCallbackFunc
   provided. Therefore none of the sync related functions have return values. */

/* Trigger a sync with a given peer */
//export InitSync
func InitSync(handle C.LogSyncHandle, peer C.PeerAddr) {
	// TODO
}

/* Provide an incoming message to the library to process
   This function releases data in msg. */
//export HandleMsg
func HandleMsg(handle C.LogSyncHandle, peer C.PeerAddr, msg C.Msg) {
	// TODO
}

// empty main func required to compile to a shared library
func main() {
}
