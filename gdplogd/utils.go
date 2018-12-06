package gdplogd

import (
	"fmt"
)

func ReadableAddr(addr HashAddr) string {
	return fmt.Sprintf("%X", addr)[:4]
	//return binary.BigEndian.Uint64(addr[:])
}
