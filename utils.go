package main

// #include "gdp_types.h"
// #include "gdp_helper.h"
import "C"

import (
    "github.com/tonyyanga/gdp-replicate/gdp"
)

func peerAddrToHash(addr C.PeerAddr) gdp.Hash {
    // TODO
    return gdp.NullHash
}

func toCMsg(msg interface{}) C.Msg {
    // TODO
    return C.Msg{}
}

func toGoMsg(msg C.Msg) interface{} {
    // TODO
    return nil
}
