package logserver

import (
	"fmt"

	"github.com/tonyyanga/gdp-replicate/gdp"
)

func main() {
	fmt.Println("vim-go")
}

type LogServer interface {
	ReadMetadata(hashes []gdp.Hash) ([]gdp.Metadatum, error)
	ReadAllMetadata() ([]gdp.Metadatum, error)
	ReadRecords(hashes []gdp.Hash) ([]gdp.Record, error)
	ReadAllRecords() ([]gdp.Record, error)
	WriteRecords(records []gdp.Record) error
}

type SearchableLogServer interface {
	LogServer

	// This function searches for records that have PrevHash = id
	FindNextRecords(id gdp.Hash) ([]gdp.Metadatum, error)
}
