package main

// #include "gdp_types.h"
// #include "gdp_helper.h"
import "C"

import (
    "unsafe"
    "bytes"
    "encoding/gob"

	"github.com/tonyyanga/gdp-replicate/gdp"
	"github.com/tonyyanga/gdp-replicate/policy"
)

func peerAddrToHash(addr C.PeerAddr) gdp.Hash {
    var ret gdp.Hash
    copy(ret[:], C.GoBytes(unsafe.Pointer(&addr.addr), 32))
    return ret
}

func toCMsg(msg interface{}) C.Msg {
    dest := &bytes.Buffer{}

    encoder := gob.NewEncoder(dest)
    gob.Register(&policy.NaiveMsgContent{})
    gob.Register(&policy.GraphMsgContent{})

    err := encoder.Encode(msg)
    if err != nil {
        panic(err)
    }

    // convert dest to a c array
    length := dest.Len()
    destBytes := dest.Bytes()
    destCArray := C.malloc(C.size_t(C.int(length)))
    copy(C.GoBytes(unsafe.Pointer(destCArray), C.int(length)), destBytes[:])

	return C.Msg{
        length: C.uint(length),
        data: destCArray,
    }
}

func toGoMsg(msg C.Msg) interface{} {
    // convert a c array to a buffer
    length := msg.length
    srcCArray := msg.data

    var srcBytes []byte
    copy(srcBytes, C.GoBytes(unsafe.Pointer(srcCArray), C.int(length)))

    src := bytes.NewBuffer(srcBytes)

    decoder := gob.NewDecoder(src)
    gob.Register(&policy.NaiveMsgContent{})
    gob.Register(&policy.GraphMsgContent{})

    var resp interface{}

    err := decoder.Decode(resp)
    if err != nil {
        panic(err)
    }

    return resp
}
