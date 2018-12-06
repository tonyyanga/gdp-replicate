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

	listenPort := os.Args[1]
	selfAddr := gdplogd.PortToHashAddr(listenPort)

	peerPort := os.Args[2]
	peerAddr := gdplogd.PortToHashAddr(peerPort)

	sqlFile := os.Args[3]

	peerMap := make(map[gdplogd.HashAddr]string)
	peerMap[peerAddr] = peerPort

	daemon.InitLogger(selfAddr)

	d, err := daemon.NewDaemon(listenPort, sqlFile, selfAddr, peerMap)
	if err != nil {
		panic(err)
	}

	err = d.Start()
	if err != nil {
		panic(err)
	}

}
