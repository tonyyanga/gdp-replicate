package main

import (
	"os"

	"github.com/tonyyanga/gdp-replicate/daemon"
	"github.com/tonyyanga/gdp-replicate/gdplogd"
)

func main() {
	listenAddr := os.Args[1]
	peerAddr := os.Args[2]

	var peer gdplogd.HashAddr

	sqlFile := "gdplogd/sample.glog"

	peerMap := make(map[gdplogd.HashAddr]string)
	peerMap[peer] = peerAddr

	d, err := daemon.NewDaemon(listenAddr, sqlFile, peer /* same address in a pair */, peerMap)
	if err != nil {
		panic(err)
	}

	err = d.Start()
	if err != nil {
		panic(err)
	}

}
