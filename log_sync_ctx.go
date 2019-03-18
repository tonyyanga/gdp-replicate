package main

/* This file maintains package level context for log syncing */

// #include "gdp_types.h"
import "C"

import (
    "errors"
    "math/rand"
)

type HandleTicket = uint32

type LogSyncCtx struct {
    // TODO
}

// Global map from handleTicket in LogSyncHandle to Go context
var logCtxMap map[HandleTicket]LogSyncCtx

func newLogSyncCtx() LogSyncCtx {
    return LogSyncCtx{}
    // TODO
}

// Helper func to get log sync ctx from map
func getLogSyncCtx(handle C.LogSyncHandle) (*LogSyncCtx, error) {
    ticket := uint32(handle.handleTicket)

    result, ok := logCtxMap[ticket]
    if !ok {
        return nil, errors.New("Undefined log sync handle")
    } else {
        return &result, nil
    }
}

// Generate random ticket not in the map
func generateHandleTicket() HandleTicket {
    for {
        ticket := rand.Uint32()
        if _, ok := logCtxMap[ticket]; !ok {
            return ticket
        }
    }
}
