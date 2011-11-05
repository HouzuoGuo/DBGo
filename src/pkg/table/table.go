package table

import (
	"os"
	"column"
	vector "container/vector"
)

type Table struct {
	Path, Name, DefFilePath, DataFilePath, LogFilePath string
	DefFile, DataFile, LogFile *os.File
	Columns map[string]*column.Column
	RowLength int
	ColumnsOrder vector.Vector
}

func OpenTable(path, name string) (table *Table) {
	return nil
}

func (table *Table) Init() (err os.Error){
	table.RowLength = 0
	table.Columns = make(map[string]*column.Column)
	table.ColumnsOrder = vector.Vector.New()
	return nil
}

func (table *Table) OpenFileHandles() (err os.Error) {
	return nil
}