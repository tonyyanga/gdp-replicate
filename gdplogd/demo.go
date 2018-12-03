package gdplogd

import (
	"database/sql"
	"fmt"
	"log"
)

// Demonstrate the LogGraphWrapper
func Demo() {
	fmt.Println("demoing")

	var log LogGraphWrapper
	log, _ = InitFakeGraph()

	fmt.Println("Actual Pointer Map:")
	for key, val := range log.GetActualPtrMap() {
		fmt.Printf("%x -> %x\n", key, val)
	}

	fmt.Println("Logical Pointer Map:")
	for key, hashes := range log.GetLogicalPtrMap() {
		fmt.Printf("\n%x -> \n", key)
		for _, hash := range hashes {
			fmt.Printf("\t%x\n", hash)
		}
	}

	fmt.Println("Logical Begins:")
	for _, hash := range log.GetLogicalBegins() {
		fmt.Printf("%x\n", hash)
	}

	fmt.Println("Logical Ends:")
	for _, hash := range log.GetLogicalEnds() {
		fmt.Printf("%x\n", hash)
	}

}

// Demonstrate the ability to create, write to and read from a database.
func SqlDemo() {
	db, err := sql.Open("sqlite3", "./gdplogd/sample.glog")
	checkError(err)
	defer db.Close()

	var log LogGraphWrapper

	log, err = InitLogGraph([32]byte{}, db)
	checkError(err)

	fmt.Println("Logical Begins:")
	for _, hash := range log.GetLogicalBegins() {
		fmt.Printf("%x\n", hash)
	}

}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
