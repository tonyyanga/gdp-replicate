package log_database

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

type LogEntry struct {
	Hash []byte

	// monotonically increasing number that increases by 1 for each new record,
	// represents the count of records starting from the very first record
	RecNo int

	// 64 bit, nanoseconds since 1/1/70
	Timestamp int64

	// in seconds (single precision)
	Accuracy float64
	PrevHash []byte
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

// Return all log entries in the database
func GetAllLogs(db *sql.DB) ([]LogEntry, error) {
	rows, err := db.Query("select hash, recno, timestamp, accuracy, prevhash, value, sig from log_entry")
	if err != nil {
		return nil, err
	}

	var logEntries []LogEntry
	for rows.Next() {
		var logEntry LogEntry
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
			// Return the log entries read so far with the error
			return logEntries, err
		}

		logEntries = append(logEntries, logEntry)
	}
	return logEntries, nil
}

// Determine if a log entry with a specific hash is present in the database
func HashPresent(db *sql.DB, hash []byte) (bool, error) {
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
