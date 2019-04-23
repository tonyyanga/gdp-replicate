package main

import (
    "testing"

    "github.com/tonyyanga/gdp-replicate/policy"
    "github.com/tonyyanga/gdp-replicate/gdp"
)

func TestMsgConv(t *testing.T) {
    nullHash := []gdp.Hash{gdp.NullHash}
    msg := &policy.GraphMsgContent{
        Num: 1,
        LogicalBegins: nullHash,
        LogicalEnds: nullHash,
        HashesTXWants: nullHash,
        RecordsNotInRX: []gdp.Record{},
    }

    cMsg := toCMsg(msg)

    toGoMsg(cMsg)
}
