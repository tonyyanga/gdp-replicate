package daemon

import (
	"fmt"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"github.com/tonyyanga/gdp-replicate/policy"
)

// msgPrinter is a message handler that displays the msg.
func msgPrinter(src gdplogd.HashAddr, msg *policy.Message) {
	fmt.Printf("received %s\n", *msg)
}
