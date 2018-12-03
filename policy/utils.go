package policy

import (
	"bytes"
	"io"
	"strconv"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
)

// Write 32 bytes hash addresses consectively to the bytes.Writer given
// First, length of the array will be written, followed by a \n
// Then the byte array is written
func addrListToReader(addrs []gdplogd.HashAddr, buf *bytes.Buffer) {
	buf.WriteString(strconv.Itoa(len(addrs)))
	buf.WriteString("\n")
	for _, addr := range addrs {
		buf.Write(addr[:])
	}
}

// Read 32 bytes hash addresses consectively from Reader
// First, length of the array will be read, followed by a \n
// Then the byte array is read and returned
func addrListFromReader(buf io.Reader) ([]gdplogd.HashAddr, error) {
	// TODO
	return nil, nil
}
