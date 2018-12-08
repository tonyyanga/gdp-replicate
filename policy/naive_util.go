package policy

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"

	"github.com/tonyyanga/gdp-replicate/gdplogd"
	"go.uber.org/zap"
)

// GetAllLogHashes returns a slice of all log hashes in the log server
func GetAllLogHashes(db *sql.DB) ([]gdplogd.HashAddr, error) {
	allHashes := []gdplogd.HashAddr{}

	queryString := "select hash from log_entry;"
	rows, err := db.Query(queryString)
	if err != nil {
		return nil, err
	}

	var hash gdplogd.HashAddr
	var hashHolder []byte
	for rows.Next() {
		err = rows.Scan(&hashHolder)
		if err != nil {
			return nil, err
		}

		// Copy the byte slices into byte arrays
		copy(hash[:], hashHolder[0:32])

		allHashes = append(allHashes, hash)
	}
	return allHashes, nil
}

func WriteLogEntry(db *sql.DB, logEntries []LogEntry) error {
	insert_statement := `insert into log_entry(
		hash, recno, timestamp, accuracy, prevhash, value, sig) 
		values(?, ?, ?, ?, ?, ?, ?);`

	tx, err := db.Begin()
	insert, err := tx.Prepare(insert_statement)
	defer insert.Close()

	if err != nil {
		return err
	}

	for _, storedLogEntry := range logEntries {
		_, err = insert.Exec(
			storedLogEntry.Hash[:],
			storedLogEntry.RecNo,
			storedLogEntry.Timestamp,
			storedLogEntry.Accuracy,
			storedLogEntry.PrevHash[:],
			storedLogEntry.Value,
			storedLogEntry.Sig,
		)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func GetLogEntries(db *sql.DB, hashes []gdplogd.HashAddr) ([]LogEntry, error) {
	logEntries := []LogEntry{}
	for _, hash := range hashes {
		logEntry, err := GetLogEntry(db, hash)
		if err != nil {
			zap.S().Errorw(
				"Failed to load log entry",
				"error", err.Error(),
				"hash", gdplogd.ReadableAddr(hash),
			)
		}
		logEntries = append(logEntries, *logEntry)
	}
	return logEntries, nil
}

func GetLogEntry(db *sql.DB, hash gdplogd.HashAddr) (*LogEntry, error) {
	var logEntry LogEntry

	queryString := fmt.Sprintf("select hash, recno, timestamp, accuracy, prevhash, value, sig from log_entry where hex(hash) == '%X'", hash)
	rows, err := db.Query(queryString)
	if err != nil {
		return nil, err
	}

	var hashHolder []byte
	var prevHashHolder []byte
	for rows.Next() {
		err = rows.Scan(
			&hashHolder,
			&logEntry.RecNo,
			&logEntry.Timestamp,
			&logEntry.Accuracy,
			&prevHashHolder,
			&logEntry.Value,
			&logEntry.Sig,
		)
		if err != nil {
			return nil, err
		}

		// Copy the byte slices into byte arrays
		copy(logEntry.Hash[:], hashHolder[0:32])

		// Previous hashes may not be populated
		if len(prevHashHolder) > 0 {
			copy(logEntry.PrevHash[:], prevHashHolder[0:32])
		}
	}

	return &logEntry, nil
}

func findDifferences(myHashes, theirHashes []gdplogd.HashAddr) (onlyMine []gdplogd.HashAddr, onlyTheirs []gdplogd.HashAddr) {
	mySet := initSet(myHashes)
	theirSet := initSet(theirHashes)

	for myHash := range mySet {
		_, present := theirSet[myHash]
		if !present {
			onlyMine = append(onlyMine, myHash)
		}
	}
	for theirHash := range theirSet {
		_, present := mySet[theirHash]
		if !present {
			onlyTheirs = append(onlyTheirs, theirHash)
		}
	}

	return onlyMine, onlyTheirs
}

func initSet(hashes []gdplogd.HashAddr) map[gdplogd.HashAddr]bool {
	set := make(map[gdplogd.HashAddr]bool)
	for _, hash := range hashes {
		set[hash] = false
	}
	return set
}

func createSecondMessageReader(logEntries []LogEntry, hashes []gdplogd.HashAddr) *bytes.Buffer {
	var buf bytes.Buffer

	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(logEntries); err != nil {
		zap.S().Errorw(
			"Error encoding log entries",
			"error", err.Error(),
		)
	}

	if err := enc.Encode(hashes); err != nil {
		zap.S().Errorw(
			"Error encoding log entries",
			"error", err.Error(),
		)
	}

	return &buf
}
