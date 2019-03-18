package main

// #include "gdp_types.h"
// #include "gdp_helper.h"
import "C"

type MsgCallback func(C.PeerAddr, C.Msg)

func createMsgCallback(f C.MsgCallbackFunc) MsgCallback {
    return func(peer C.PeerAddr, msg C.Msg) {
        C.bridgeMsgCallbackFunc(f, peer, msg)
    }
}
