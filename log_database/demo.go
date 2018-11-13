package log_database

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"log"
	"os"
)

// Demonstrate the ability to create, write to and read from a database.
func Demo() {
	os.Remove("./foo.db")

	db, err := sql.Open("sqlite3", "./foo.db")
	checkError(err)
	defer db.Close()

	checkError(CreateTable(db))

	// create an example log entry
	storedHash := make([]byte, 4)
	rand.Read(storedHash)
	storedPrevHash := make([]byte, 4)
	rand.Read(storedPrevHash)

	storedLogEntry := LogEntry{
		Hash:      storedHash,
		RecNo:     1,
		Timestamp: 2,
		Accuracy:  3.0,
		PrevHash:  storedPrevHash,
		Value:     []byte("some value"),
		Sig:       []byte("some signature"),
	}
	logEntries := []LogEntry{storedLogEntry}

	checkError(AppendLogEntry(db, logEntries))

	rows, err := db.Query("select hash, recno from log_entry")
	checkError(err)
	defer rows.Close()

	for rows.Next() {
		var recNo int
		var hash []byte

		err = rows.Scan(&hash, &recNo)
		checkError(err)

		fmt.Printf("hash: %x\n", hash)
		fmt.Printf("recno: %d\n", recNo)
	}
	checkError(rows.Err())
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
