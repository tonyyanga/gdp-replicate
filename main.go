package main

import (
	"fmt"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"github.com/tonyyanga/gdp-replicate/log_database"
	"github.com/tonyyanga/gdp-replicate/policy"
)

func main() {
	// Just to test import
	var msgType policy.MessageType
	msgType = 1

	var addr gdplogd.HashAddr

	fmt.Println(msgType)
	fmt.Println(addr)
	log_database.Demo()
}
