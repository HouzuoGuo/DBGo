package table

import (
	"os"
	"strings"
	"strconv"
	"column"
	"constant"
	"util"
)

type Table struct {
	Path, Name, DefFilePath, DataFilePath string
	DefFile, DataFile *os.File
	Columns map[string]*column.Column
	RowLength int
	NumberOfColumns int
	ColumnsInOrder []*column.Column
}

func Open(path, name string) (table *Table, err os.Error) {
	table.Path = path
	table.Name = name
	err = table.Init()
	return
}

func (table *Table) Init() (err os.Error) {
	table.RowLength = 0
	table.NumberOfColumns = 0
	table.Columns = make(map[string]*column.Column)
	table.ColumnsInOrder = make([]*column.Column, 0)
	err = table.OpenFiles()
	if err != nil {
		return
	}
	defFileInfo, err := table.DefFile.Stat()
	if err != nil {
		return
	}
	content := make([]byte, defFileInfo.Size)
	lines := strings.Split(string(content), "\n")
	table.NumberOfColumns = len(lines)
	for _, line := range lines {
		lengthName := strings.Split(line, ":")
		var length int
		length, err = strconv.Atoi(lengthName[0])
		aColumn := &column.Column{Offset:table.RowLength, Length:length, Name:lengthName[1]}
		table.Columns[lengthName[1]] = aColumn
		table.ColumnsInOrder = append(table.ColumnsInOrder[:], aColumn)
		table.RowLength += length
	}
	table.RowLength++
	return
}

func (table *Table) OpenFiles() (err os.Error) {
	table.DefFile, err = os.OpenFile(table.DefFilePath, os.O_RDWR, constant.DataFilePerm)
	if err == nil {
		table.DataFile, err = os.OpenFile(table.DataFilePath, os.O_RDWR, constant.DataFilePerm)
	}
	return
}

func (table *Table) Flush() (ok bool){
	err := table.DefFile.Sync()
	if err == nil {
		err = table.DataFile.Sync()
		ok = true
	}
	return
}

func (table *Table) Seek(rowNumber int) (ok bool) {
	numberOfRows, ok := table.NumberOfRows()
	if ok && rowNumber < numberOfRows {
		table.DataFile.Seek(int64(rowNumber * table.RowLength), 0)
		ok = true
	}
	return
}

func (table *Table) SeekColumn(rowNumber int, columnName string) (ok bool) {
	if table.Seek(rowNumber) == true {
		column, exists := table.Columns[columnName]
		if exists {
			_, err := table.DataFile.Seek(int64(column.Offset), 1)
			if err == nil {
				ok = true
			}
		}
	}
	return
}

func (table *Table) NumberOfRows() (numberOfRows int, ok bool) {
	var dataFileInfo *os.FileInfo
	dataFileInfo, err := table.DataFile.Stat()
	if err == nil {
		numberOfRows = int(dataFileInfo.Size)
		ok = true
	}
	return
}

func (table *Table) Read(rowNumber int) (row map[string]string, ok bool) {
	if table.Seek(rowNumber) {
		rowInBytes := make([]byte, table.NumberOfColumns)
		_, err := table.DataFile.Read(rowInBytes)
		if err == nil {
			for _, column := range table.ColumnsInOrder {
				row[column.Name] = string(rowInBytes[column.Offset:column.Offset+column.Length])
			}
			ok = true
		}
	}
	return
}

func (table *Table) Write(column *column.Column, value string) (ok bool){
	_, err := table.DataFile.WriteString(util.TrimLength(value, column.Length))
	if err == nil {
		ok = true
	}
	return
}

func (table *Table) Insert(row map[string]string) (ok bool) {
	_, err := table.DataFile.Seek(0, 2)
	if err == nil {
		for _, column := range table.ColumnsInOrder {
			value, exists := row[column.Name]
			if !exists {
				value = ""
			} 
			if !table.Write(column, value) {
				return
			}
		}
		_, err = table.DataFile.WriteString("\n")
		if err == nil {
			ok = true
		}
	}
	return
}

func (table *Table) Delete(rowNumber int) (ok bool) {
	if table.Seek(rowNumber) {
		del, exists := table.Columns["~del"]
		if exists {
			ok = table.Write(del, "y") 
		}
	}
	return
}

func (table *Table) Update(rowNumber int, row map[string]string) (ok bool) {
	if table.Seek(rowNumber) {
		for columnName, value := range row {
			column, exists := table.Columns[columnName]
			if exists {
				if !table.Write(column, value) {
					return
				}
			} else {
				return
			}
		}
		ok = true
	}
	return
}

func (table *Table) RebuildDataFile() (ok bool) {
	return
}