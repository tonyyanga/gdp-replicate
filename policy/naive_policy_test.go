package policy

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tonyyanga/gdp-replicate/gdplogd"
)

const SQL_FILE = "/home/scott/go/src/github.com/tonyyanga/gdp-replicate/gdplogd/sample.glog"

func TestGetAllLogHashes(t *testing.T) {
	db, err := sql.Open("sqlite3", SQL_FILE)
	assert.Nil(t, err)
	defer db.Close()

	hashes, err := GetAllLogHashes(db)
	assert.Nil(t, err)
	assert.NotEqual(t, 0, len(hashes))
	for hash := range hashes {
		t.Logf("%X\n", hash)
	}
}

func aTestWriteLogEntry(t *testing.T) {
	db, err := sql.Open("sqlite3", SQL_FILE)
	assert.Nil(t, err)
	defer db.Close()

	logEntries := []LogEntry{
		LogEntry{
			Hash:      gdplogd.HashAddr{},
			RecNo:     1,
			Timestamp: 2,
			Accuracy:  2,
			PrevHash:  gdplogd.HashAddr{},
			Value:     []byte{},
			Sig:       []byte{},
		},
	}
	assert.Nil(t, WriteLogEntry(db, logEntries))
}

func TestReadLogEntry(t *testing.T) {
	db, err := sql.Open("sqlite3", SQL_FILE)
	assert.Nil(t, err)
	defer db.Close()

	logEntry, err := GetLogEntry(db, gdplogd.HashAddr{})
	assert.Nil(t, err)
	assert.Equal(t, 1, logEntry.RecNo)
}

func TestBinaryConversion(t *testing.T) {
	hash := gdplogd.HashAddr{}
	hash[0] = 1
	prevHash := gdplogd.HashAddr{}
	prevHash[1] = 1
	logEntry := LogEntry{
		Hash:      hash,
		RecNo:     1,
		Timestamp: 2,
		Accuracy:  2,
		PrevHash:  prevHash,
		Value:     []byte{},
		Sig:       []byte{},
	}
	newLogEntry := LogEntry{}
	data, err := logEntry.MarshalBinary()

	assert.Nil(t, err)
	assert.Nil(t, newLogEntry.UnmarshalBinary(data))
	assert.Equal(t, logEntry.Hash, newLogEntry.Hash)
	assert.Equal(t, logEntry.PrevHash, newLogEntry.PrevHash)
	lgs := []LogEntry{logEntry}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	assert.Nil(t, enc.Encode(lgs))

}
