package main

// #include "gdp_types.h"
// #include "gdp_helper.h"
import "C"

import (
    "unsafe"
)

import (
	"github.com/tonyyanga/gdp-replicate/gdp"
)

func peerAddrToHash(addr C.PeerAddr) gdp.Hash {
    var ret gdp.Hash
    copy(ret[:], C.GoBytes(unsafe.Pointer(&addr.addr), 32))
    return ret
}

func toCMsg(msg interface{}) C.Msg {
	// TODO
	return C.Msg{}
}

func toGoMsg(msg C.Msg) interface{} {
	// TODO
	return nil
}
