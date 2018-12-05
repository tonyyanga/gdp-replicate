package daemon

import (
	"fmt"
	"log"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"github.com/tonyyanga/gdp-replicate/policy"
	"go.uber.org/zap"
)

// msgPrinter is a message handler that displays the msg.
func msgPrinter(src gdplogd.HashAddr, msg *policy.Message) {
	fmt.Printf("received message")
}

// InitLogger initializes the Zap logger
func InitLogger() {
	zapLogger, err := zap.NewProduction()
	if err != nil {
		log.Fatal("failed to create logger:", err.Error())
	}
	zap.ReplaceGlobals(zapLogger)

}
