package logserver

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/tonyyanga/gdp-replicate/gdp"
	"go.uber.org/zap"
)

// SqliteServer implements SnapshotLogServer interface
// time is represented as rowid, a builtin column of sqlite
// snapshot time is inclusive of exact record at time
type SqliteServer struct {
	db *sql.DB
}

func NewSqliteServer(db *sql.DB) *SqliteServer {
	return &SqliteServer{db: db}
}

func (s *SqliteServer) CreateSnapshot() (*Snapshot, error) {
	// Use a readonly transaction to get logicalStarts and logicalEnds
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}

	// We don't write to the db, so we rollback as always
	defer func() { tx.Rollback() }()

	queryString := "SELECT max(rowid) from log_entry"
	rows, err := tx.Query(queryString)
	if err != nil {
		return nil, err
	}

	rowids, err := parseIntRows(rows)
	if err != nil {
		return nil, err
	}

	if len(rowids) != 1 {
		panic("Unexpected max row id from query")
	}

	maxRowId := rowids[0]

	// Logical starts
	queryString = `
    SELECT hash, recno, timestamp, accuracy, prevhash, sig
    FROM log_entry
    WHERE NOT hash IN
        (SELECT log1.hash as hash
         FROM log_entry log1
         JOIN log_entry log2
         ON log1.prevhash = log2.hash
        )`

	rows, err = tx.Query(queryString)
	if err != nil {
		return nil, err
	}

	startsMetadata, err := parseMetadataRows(rows)
	if err != nil {
		return nil, err
	}

	starts := make(map[gdp.Hash][]gdp.Hash)
	for _, meta := range startsMetadata {
		starts[meta.Hash] = []gdp.Hash{meta.PrevHash}
	}

	queryString = `
    SELECT hash, recno, timestamp, accuracy, prevhash, sig
    FROM log_entry
    WHERE NOT hash IN
        (SELECT log2.hash as hash
         FROM log_entry log1
         JOIN log_entry log2
         ON log1.prevhash = log2.hash
        )`

	rows, err = tx.Query(queryString)
	if err != nil {
		return nil, err
	}

	endsMetadata, err := parseMetadataRows(rows)
	if err != nil {
		return nil, err
	}

	ends := make(map[gdp.Hash]bool)
	for _, meta := range endsMetadata {
		ends[meta.Hash] = true
	}

	return &Snapshot{
		time:          maxRowId,
		logServer:     s,
		newRecords:    make(map[gdp.Hash]bool),
		logicalStarts: starts,
		logicalEnds:   ends,
	}, nil
}

func (s *SqliteServer) DestroySnapshot(*Snapshot) {}

func (s *SqliteServer) CheckRecordExistence(time int64, id gdp.Hash) (bool, error) {
	hexHash := fmt.Sprintf("x'%X'", id)

	queryString := fmt.Sprintf(
		"SELECT count(*) FROM log_entry WHERE hash = %s and rowid <= %d",
		hexHash,
		time,
	)
	rows, err := s.db.Query(queryString)
	if err != nil {
		return false, err
	}

	cnt, err := parseIntRows(rows)
	if err != nil {
		return false, err
	}
	if len(cnt) != 1 {
		panic("Unexpected count from query")
	}

	return cnt[0] > 0, nil
}

// ReadRecords will retrieive the metadat of records with specified
// hashes from the database.
func (s *SqliteServer) ReadMetadata(hashes []gdp.Hash) ([]gdp.Metadatum, error) {
	if len(hashes) == 0 {
		return nil, nil
	}

	hexHashes := make([]string, 0, len(hashes))
	for _, hash := range hashes {
		hexHashes = append(hexHashes, fmt.Sprintf("x'%X'", hash))
	}

	queryString := fmt.Sprintf(
		"SELECT hash, recno, timestamp, accuracy, prevhash, sig FROM log_entry WHERE hash IN (%s)",
		strings.Join(hexHashes, ","),
	)
	fmt.Println(queryString)
	rows, err := s.db.Query(queryString)
	if err != nil {
		return nil, err
	}

	return parseMetadataRows(rows)
}

// ReadRecords will retrieive the records with specified hashes from
// the database.
func (s *SqliteServer) ReadRecords(hashes []gdp.Hash) ([]gdp.Record, error) {
	if len(hashes) == 0 {
		return nil, nil
	}

	hexHashes := make([]string, 0, len(hashes))
	for _, hash := range hashes {
		hexHashes = append(hexHashes, fmt.Sprintf("x'%X'", hash))
	}

	queryString := fmt.Sprintf(
		"SELECT hash, recno, timestamp, accuracy, prevhash, value, sig FROM log_entry WHERE hash IN (%s)",
		strings.Join(hexHashes, ","),
	)
	rows, err := s.db.Query(queryString)
	if err != nil {
		return nil, err
	}

	records, err := parseRecordRows(rows)
	if err != nil {
		return nil, err
	}

	return records, nil
}

// SearchableLogServer interface
func (s *SqliteServer) FindNextRecords(id gdp.Hash) ([]gdp.Metadatum, error) {
	hexHash := fmt.Sprintf("x'%X'", id)

	queryString := fmt.Sprintf(
		"SELECT hash, recno, timestamp, accuracy, prevhash, sig FROM log_entry WHERE prevhash = %s",
		hexHash,
	)
	rows, err := s.db.Query(queryString)
	if err != nil {
		return nil, err
	}

	metadata, err := parseMetadataRows(rows)
	if err != nil {
		return nil, err
	}

	return metadata, nil
}

// ReadAllRecords will retrieve all records from the database.
func (s *SqliteServer) ReadAllMetadata() ([]gdp.Metadatum, error) {
	queryString := "SELECT hash, recno, timestamp, accuracy, prevhash, sig FROM log_entry"

	rows, err := s.db.Query(queryString)
	if err != nil {
		return nil, err
	}

	metadata, err := parseMetadataRows(rows)
	if err != nil {
		return nil, err
	}

	return metadata, nil
}

// ReadAllRecords will retrieve all records from the database.
func (s *SqliteServer) ReadAllRecords() ([]gdp.Record, error) {
	queryString := "SELECT hash, recno, timestamp, accuracy, prevhash, value, sig FROM log_entry"

	rows, err := s.db.Query(queryString)
	if err != nil {
		return nil, err
	}

	records, err := parseRecordRows(rows)
	if err != nil {
		return nil, err
	}

	return records, nil
}

// WriteRecords will write all records to the database.
func (s *SqliteServer) WriteRecords(records []gdp.Record) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT INTO log_entry (hash, recno, timestamp, accuracy, prevhash, value, sig) VALUES (?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, record := range records {
		_, err = stmt.Exec(
			record.Hash[:],
			record.RecNo,
			record.Timestamp,
			record.Accuracy,
			record.PrevHash[:],
			record.Value,
			record.Sig,
		)
		if err != nil {
			return err
		}

	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	zap.S().Infow(
		"Wrote records",
		"numRecords", len(records),
	)

	return nil
}
