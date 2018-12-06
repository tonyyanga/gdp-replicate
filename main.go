package main

import (
	"crypto/sha256"
	"os"
	"strconv"
	"strings"

	"github.com/tonyyanga/gdp-replicate/daemon"
	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"go.uber.org/zap"
)

func main() {
	if len(os.Args) < 4 {
		panic("Requires arguments: SQL file, listen address, peer address, fanout degree")
	}

	sqlFile := os.Args[1]

	listenAddr := os.Args[2]
	selfGDPAddr := sha256.Sum256([]byte(listenAddr))

	daemon.InitLogger(selfGDPAddr)
	peerMap := parsePeers(os.Args[3])

	fanoutDegree, err := strconv.Atoi(os.Args[4])
	if err != nil {
		panic("unable to parse fanout degree")
	}

	d, err := daemon.NewDaemon(listenAddr, sqlFile, selfGDPAddr, peerMap)
	if err != nil {
		panic(err)
	}

	err = d.Start(fanoutDegree)
	if err != nil {
		panic(err)
	}

}

// parsePeers parses a comma delimited string of IP:ports to a map from
// GDP addr to IP addr.
func parsePeers(peers string) map[gdplogd.HashAddr]string {
	peerMap := make(map[gdplogd.HashAddr]string)
	peerAddrs := strings.Split(peers, ",")
	for _, peerAddr := range peerAddrs {
		peerGDPAddr := sha256.Sum256([]byte(peerAddr))
		peerMap[peerGDPAddr] = peerAddr
	}

	for gdpAddr, httpAddr := range peerMap {
		zap.S().Infow(
			"Added peer",
			"gdpAddr", gdplogd.ReadableAddr(gdpAddr),
			"httpAddr", httpAddr,
		)
	}
	return peerMap

}
