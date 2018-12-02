package gdplogd

import (
	"log"
)

// Demonstrate the ability to create, write to and read from a database.
func Demo() {

	/*
		db, err := sql.Open("sqlite3", "./log_database/sample.glog")
		checkError(err)
		defer db.Close()

		logEntries, err := GetAllLogs(db)
		checkError(err)
		hash := logEntries[0].Hash

		present, err := HashPresent(db, hash)
		checkError(err)
		fmt.Printf("hash present: %t\n", present)

		forwardEdges, backwardEdges := GetLogGraphs(logEntries)
		for k, v := range backwardEdges {
			fmt.Printf("backward edge %X -> %X\n", k, v)
		}
		for k, v := range forwardEdges {
			fmt.Printf("forward edge %X -> %X\n", k, v)
		}
	*/
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
