package table

import (
	"os"
	"column"
)

type Table struct {
	Path, Name, DefFilePath, DataFilePath, LogFilePath string
	DefFile, DataFile, LogFile *os.File
	Columns map[string]*column.Column
	RowLength int
	ColumnsOrder []*column.Column
}

func OpenTable(path, name string) (table *Table) {
	return nil
}

func (table *Table) OpenFileHandles() (err os.Error) {
}