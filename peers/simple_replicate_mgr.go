package peers

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"github.com/tonyyanga/gdp-replicate/policy"
)

// Simple Replication Manager that directly connects to peers
type SimpleReplicateMgr struct {
	// Store the IP:Port address for each peer
	PeerAddrMap map[HashAddr]string
}

// Constructor for SimpleReplicateMgr
func NewSimpleReplicateMgr(peerAddrMap map[HashAddr]string) *SimpleReplicateMgr {
	return &SimpleReplicateMgr{
		PeerAddrMap: peerAddrMap,
	}
}

func (mgr *SimpleReplicateMgr) ListenAndServe(address string, handler func(msg *policy.Message)) error {
	// msgHandler translates HTTP to messages
	msgHander := func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "POST" {
			http.Error(w, "Only POST requests are supported", 500)
			return
		}

		msgTypeStr := req.Header.Get("MessageType")
		if msgTypeStr == "" {
			http.Error(w, "Expect MessageType in header", 500)
			return
		}

		msgType, err := strconv.Atoi(msgTypeStr)
		if err != nil {
			http.Error(w, "Corrupted MessageType in header", 500)
			return
		}

		msg := &policy.Message{
			Type: msgType,
			Body: req.Body,
		}

		io.WriteString(w, "Accepted")
	}

	http.HandleFunc("/", msgHandler)
	return http.ListenAndServe(address, nil)
}

func (mgr *SimpleReplicateMgr) Send(peer gdplogd.HashAddr, msg *policy.Message) error {
	// Look up peer's actual IP address
	ipAddr, ok := mgr.PeerAddrMap[peer]
	if !ok {
		return fmt.Errorf("Cannot find peer's address: %v", peer)
	}

	c := &http.Client{}

	// Construct HTTP request
	req := http.NewRequest("POST", "http://"+ipAddr, msg.Body)
	req.Header.Add("MessageType", fmt.Sprint(msg.Type))

	resp, err := c.Do(c)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("Response code is not 200: %v", resp.StatusCode)
	}

	return nil
}

func (mgr *SimpleReplicateMgr) Broadcast(msg *policy.Message) map[gdplogd.HashAddr]error {
	// Dispatch several Send at the same time
	ret := &sync.Map{}
	wg := &sync.WaitGroup{}

	for k := range mgr.PeerAddrMap {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := mgr.Send(peer, msg)
			ret.Store(k, err)
		}()
	}

	wg.Wait()
	return ret
}
