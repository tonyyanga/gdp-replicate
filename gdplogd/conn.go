package gdplogd

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
)

// Represent a connection to a log daemon
type LogDaemonConnection interface {
	GetGraphs() (map[string]LogGraphWrapper, error)

	GetGraph(name string) (*LogGraphWrapper, error)

	ReadLogMetadata(name string, addr HashAddr) (*LogEntryMetadata, error)
	ReadLogItem(name string, addr HashAddr) (io.Reader, error)

	WriteLogItem(name string, metadata *LogEntryMetadata, data io.Reader) error

	ContainsLogItem(name string, addr HashAddr) (bool, error)
}

// Supports only a single database per instance
type LogDaemonConnector struct {
	db     *sql.DB
	graphs map[string]LogGraphWrapper
}

// Initialize LogDaemonConnector and its LogGraph
func InitLogDaemonConnector(db *sql.DB) (LogDaemonConnector, error) {
	var conn LogDaemonConnector
	conn.db = db
	conn.graphs = make(map[string]LogGraphWrapper)

	logGraph, err := InitLogGraph(HashAddr{}, conn.db)
	if err != nil {
		return conn, err
	}
	conn.graphs["default"] = logGraph
	return conn, nil
}

func (conn LogDaemonConnector) GetGraphs() (map[string]LogGraphWrapper, error) {
	return conn.graphs, nil
}

func (conn LogDaemonConnector) GetGraph(name string) (*LogGraphWrapper, error) {
	lgw, present := conn.graphs[name]
	if !present {
		return nil, errors.New("Missing key for graph")
	}
	return &lgw, nil
}

// Return the log entry metadata with ADDR from database with NAME.
func (conn LogDaemonConnector) ReadLogMetadata(
	name string,
	addr HashAddr,
) (*LogEntryMetadata, error) {
	var logEntry LogEntryMetadata

	queryString := fmt.Sprintf(
		"select hash, recno, timestamp, accuracy, prevhash, sig from log_entry where hex(hash) == '%X'",
		addr,
	)
	rows, err := conn.db.Query(queryString)
	if err != nil {
		return &logEntry, err
	}

	for rows.Next() {
		err = rows.Scan(
			&logEntry.Hash,
			&logEntry.RecNo,
			&logEntry.Timestamp,
			&logEntry.Accuracy,
			&logEntry.PrevHash,
			&logEntry.Sig,
		)
		if err != nil {
			return &logEntry, err
		}
	}

	return &logEntry, nil
}

// Return the log entry value with ADDR from database with NAME.
func (conn LogDaemonConnector) ReadLogItem(name string, addr HashAddr) (io.Reader, error) {
	var value []byte

	queryString := fmt.Sprintf("select value from log_entry where hex(hash) == '%X'", addr)
	rows, err := conn.db.Query(queryString)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		err = rows.Scan(&value)
		if err != nil {
			return nil, err
		}
	}

	return bytes.NewReader(value), nil
}

// Add LogEntries to the database.
func (conn LogDaemonConnector) WriteLogItem(name string, logEntry *LogEntryMetadata, data io.Reader) error {
	tx, err := conn.db.Begin()
	if err != nil {
		return err
	}

	insert_statement := `insert into log_entry(
		hash, recno, timestamp, accuracy, prevhash, value, sig) 
		values(?, ?, ?, ?, ?, ?);`
	insert, err := tx.Prepare(insert_statement)
	if err != nil {
		return err
	}

	defer insert.Close()

	value, err := ioutil.ReadAll(data)
	if err != nil {
		return err
	}

	_, err = insert.Exec(
		logEntry.Hash,
		logEntry.RecNo,
		logEntry.Timestamp,
		logEntry.Accuracy,
		logEntry.PrevHash,
		value,
		logEntry.Sig,
	)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// Determine if a log entry with a specific hash is present in the database
func (conn LogDaemonConnector) ContainsLogItem(name string, addr HashAddr) (bool, error) {
	queryString := fmt.Sprintf("select count(hash) from log_entry where hex(hash) == '%X'\n", addr)
	rows, err := conn.db.Query(queryString)
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
