package main

import (
	"log"
	"testing"

	"github.com/tonyyanga/gdp-replicate/gdp"
	"github.com/tonyyanga/gdp-replicate/policy"
)

func TestMsgConv(t *testing.T) {
	nullHash := []gdp.Hash{gdp.NullHash}
	msg := &policy.GraphMsgContent{
		Num:            1,
		LogicalBegins:  nullHash,
		LogicalEnds:    nullHash,
		HashesTXWants:  nullHash,
		RecordsNotInRX: []gdp.Record{},
	}

	cMsg := toCMsg(msg)

	resp := toGoMsg(cMsg)

	_, ok := resp.(*policy.GraphMsgContent)
	if !ok {
		log.Fatalf("Wrong output content: %v", resp)
	}
}
