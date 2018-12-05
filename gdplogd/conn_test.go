package gdplogd

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const SQL_FILE = "/home/scott/go/src/github.com/tonyyanga/gdp-replicate/gdplogd/sample.glog"

// Demonstrate the ability to create, write to and read from a database.
func SqlDemo() {
	db, err := sql.Open("sqlite3", SQL_FILE)
	checkError(err)
	defer db.Close()

	var log LogGraphWrapper

	logGraph, err := InitLogGraph([32]byte{}, db)
	log = &logGraph
	checkError(err)

	fmt.Println("Logical Begins:")
	for _, hash := range log.GetLogicalBegins() {
		fmt.Printf("%x\n", hash)
	}

}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func TestContainsLogItem(t *testing.T) {
	db, err := sql.Open("sqlite3", SQL_FILE)
	assert.Nil(t, err)
	defer db.Close()

	conn, err := InitLogDaemonConnector(db, "")
	assert.Nil(t, err)

	// Check empty hash not present
	present, err := conn.ContainsLogItem("", HashAddr{})
	assert.False(t, present)
	assert.Nil(t, err)

	graph, err := conn.GetGraph("")
	assert.Nil(t, err)

	hash := (*graph).GetLogicalBegins()[0]

	// Check hash is present
	present, err = conn.ContainsLogItem("", hash)
	assert.True(t, present)
	assert.Nil(t, err)

	// Read metadata
	logEntry, err := conn.ReadLogMetadata("", hash)
	require.Nil(t, err)
	assert.NotNil(t, logEntry)
	assert.Equal(t, logEntry.Hash, hash)

	logReader, err := conn.ReadLogItem("", hash)
	require.Nil(t, err)
	value, err := ioutil.ReadAll(logReader)
	assert.NotEqual(t, 0, len(value))
}

func TestGraphs(t *testing.T) {
	db, err := sql.Open("sqlite3", SQL_FILE)
	assert.Nil(t, err)
	defer db.Close()

	conn, err := InitLogDaemonConnector(db, "default")
	assert.Nil(t, err)

	graph, err := conn.GetGraph("default")
	assert.Nil(t, err)
	assert.NotNil(t, graph)
}
