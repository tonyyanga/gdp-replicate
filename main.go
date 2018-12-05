package main

import (
	"os"

	"github.com/tonyyanga/gdp-replicate/daemon"
	"github.com/tonyyanga/gdp-replicate/gdplogd"
)

func main() {
	if len(os.Args) < 4 {
		panic("Requires arguments: listen address, peer address, SQL file")
	}

	listenAddr := os.Args[1]
	peerAddr := os.Args[2]

	sqlFile := os.Args[3]

	var peer gdplogd.HashAddr

	peerMap := make(map[gdplogd.HashAddr]string)
	peerMap[peer] = peerAddr

	daemon.InitLogger()
	d, err := daemon.NewDaemon(listenAddr, sqlFile, peer /* same address in a pair */, peerMap)
	if err != nil {
		panic(err)
	}

	err = d.Start()
	if err != nil {
		panic(err)
	}

}
