package log_database

import (
	"database/sql"
	"fmt"
	"log"
)

// Demonstrate the ability to create, write to and read from a database.
func Demo() {

	db, err := sql.Open("sqlite3", "./log_database/sample.glog")
	checkError(err)
	defer db.Close()

	logEntries, err := GetAllLogs(db)
	hash := logEntries[0].Hash

	present, err := HashPresent(db, hash)
	fmt.Printf("hash present: %t\n", present)
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
