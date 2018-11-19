package log_database

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

type LogEntry struct {
	Hash [32]byte

	// monotonically increasing number that increases by 1 for each new record,
	// represents the count of records starting from the very first record
	RecNo int

	// 64 bit, nanoseconds since 1/1/70
	Timestamp int64

	// in seconds (single precision)
	Accuracy float64
	PrevHash [32]byte
	Value    []byte
	Sig      []byte
}

// Create the table as used in GDP.
func CreateTable(db *sql.DB) error {
	create_table_statement := `
    CREATE TABLE log_entry (
            hash BLOB(32) PRIMARY KEY ON CONFLICT IGNORE,
            recno INTEGER, 
            timestamp INTEGER,
            accuracy FLOAT,
            prevhash BLOB(32),
            value BLOB,
            sig BLOB);
	`
	_, err := db.Exec(create_table_statement)
	return err
}

func GetLogGraphs(logEntries []LogEntry) (forwardEdges map[[32]byte][32]byte, backwardEdges map[[32]byte][32]byte) {
	forwardEdges = make(map[[32]byte][32]byte)
	backwardEdges = make(map[[32]byte][32]byte)

	var emptyHash [32]byte
	for _, logEntry := range logEntries {
		fmt.Printf("Parsing %X\n", logEntry.Hash)
		backwardEdges[logEntry.Hash] = logEntry.PrevHash

		// Do not accept empty PrevHashes
		if logEntry.PrevHash != emptyHash {
			forwardEdges[logEntry.PrevHash] = logEntry.Hash
		}
	}
	return forwardEdges, backwardEdges

}

// Return all log entries in the database
func GetAllLogs(db *sql.DB) ([]LogEntry, error) {
	rows, err := db.Query("select hash, recno, timestamp, accuracy, prevhash, value, sig from log_entry")
	if err != nil {
		return nil, err
	}

	var logEntries []LogEntry
	var hashHolder []byte
	var prevHashHolder []byte
	for rows.Next() {
		var logEntry LogEntry
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
			// Return the log entries read so far with the error
			return logEntries, err
		}

		// Copy the byte slices into byte arrays
		copy(logEntry.Hash[:], hashHolder[0:32])

		// Previous hashes may not be populated
		if len(prevHashHolder) > 0 {
			copy(logEntry.PrevHash[:], prevHashHolder[0:32])
		}

		logEntries = append(logEntries, logEntry)
	}
	return logEntries, nil
}

// Return log entry with hash
func GetLog(db *sql.DB, hash []byte) (LogEntry, error) {
	var logEntry LogEntry

	queryString := fmt.Sprintf("select hash, recno, timestamp, accuracy, prevhash, value, sig from log_entry where hex(hash) == '%X'", hash)
	rows, err := db.Query(queryString)
	if err != nil {
		return logEntry, err
	}

	for rows.Next() {
		err = rows.Scan(
			&logEntry.Hash,
			&logEntry.RecNo,
			&logEntry.Timestamp,
			&logEntry.Accuracy,
			&logEntry.PrevHash,
			&logEntry.Value,
			&logEntry.Sig,
		)
		if err != nil {
			return logEntry, err
		}
	}

	return logEntry, nil
}

// Determine if a log entry with a specific hash is present in the database
func HashPresent(db *sql.DB, hash [32]byte) (bool, error) {
	queryString := fmt.Sprintf("select count(hash) from log_entry where hex(hash) == '%X'\n", hash)
	rows, err := db.Query(queryString)
	if err != nil {
		return false, err
	}

	var hashPresent int
	for rows.Next() {
		err = rows.Scan(&hashPresent)
		if err != nil {
			return false, err
		}
	}
	return hashPresent == 1, nil
}

// Add LogEntries to the database.
func AppendLogEntry(db *sql.DB, logEntries []LogEntry) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	insert_statement := `insert into log_entry(
		hash, recno, timestamp, accuracy, prevhash, value, sig) 
		values(?, ?, ?, ?, ?, ?, ?);`
	insert, err := tx.Prepare(insert_statement)
	if err != nil {
		return err
	}

	defer insert.Close()

	for _, storedLogEntry := range logEntries {
		_, err = insert.Exec(
			storedLogEntry.Hash,
			storedLogEntry.RecNo,
			storedLogEntry.Timestamp,
			storedLogEntry.Accuracy,
			storedLogEntry.PrevHash,
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
