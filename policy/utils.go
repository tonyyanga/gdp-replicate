package policy

import (
    "bytes"
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

