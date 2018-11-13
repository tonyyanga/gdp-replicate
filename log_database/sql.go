package log_database

import (
	"database/sql"

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
