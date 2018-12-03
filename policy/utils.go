package policy

import (
    "io"
    "bytes"
    "strconv"
    "fmt"
    "bufio"

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
}

// Process begins and ends section, includes "begins\n", "ends\n"
func processBeginsEnds(body io.Reader) ([]gdplogd.HashAddr, []gdplogd.HashAddr, error) {
    reader := bufio.NewReader(body)
    line, err := reader.ReadBytes('\n')

    if err != nil || bytes.Compare(line, []byte("begins\n")) != 0 {
        return nil, nil, fmt.Errorf("Error processing message: begins")
    }

    peerBegins, err := addrListFromReader(reader)
    if err != nil {
        return nil, nil, err
    }

    line, err = reader.ReadBytes('\n')

    if err != nil || bytes.Compare(line, []byte("ends\n")) != 0 {
        return nil, nil, fmt.Errorf("Error processing message: ends")
    }

    peerEnds, err := addrListFromReader(reader)
    if err != nil {
        return nil, nil, err
    }

    return peerBegins, peerEnds, nil
}
