package main

/* This file maintains package level context for log syncing */

// #include "gdp_types.h"
import "C"

import (
	"database/sql"
	"errors"
	"math/rand"

	"github.com/tonyyanga/gdp-replicate/loggraph"
	"github.com/tonyyanga/gdp-replicate/logserver"
	"github.com/tonyyanga/gdp-replicate/policy"
	"go.uber.org/zap"
	//"github.com/tonyyanga/gdp-replicate/logserver"
)

type HandleTicket = uint32

type LogSyncCtx struct {
	Policy    policy.Policy
	logServer logserver.LogServer
	logGraph  loggraph.LogGraph
}

// Global map from handleTicket in LogSyncHandle to Go context
var logCtxMap = make(map[HandleTicket]LogSyncCtx)

func newLogSyncCtx(sqlFile string) (HandleTicket, error) {
	db, err := sql.Open("sqlite3", sqlFile)
	if err != nil {
		zap.S().Errorw(
			"Failed to open sqlite database",
			"sqlite-file", sqlFile,
			"error", err,
		)
		return 0, err
	}
	logServer := logserver.NewSqliteServer(db)
	logGraph, err := loggraph.NewSimpleGraph(logServer)
	if err != nil {
		zap.S().Errorw(
			"Failed to create log graph",
			"sqlite-file", sqlFile,
			"error", err,
		)
		return 0, err
	}

	// TODO: support alternate policies
	policy := policy.NewGraphDiffPolicy(logGraph)

	ticket := generateHandleTicket()

	logCtxMap[ticket] = LogSyncCtx{
		logServer: logServer,
		logGraph:  logGraph,
		Policy:    policy,
	}

	return ticket, nil
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
