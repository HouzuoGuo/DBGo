package table

import (
	"os"
	"strings"
	"strconv"
	"column"
	"constant"
	"st"
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

func Open(path, name string) (table *Table, status int) {
	table.Path = path
	table.Name = name
	status = table.Init()
	return
}

func (table *Table) Init() (status int) {
	table.RowLength = 0
	table.NumberOfColumns = 0
	table.Columns = make(map[string]*column.Column)
	table.ColumnsInOrder = make([]*column.Column, 0)
	ret := table.OpenFiles()
	if ret != st.OK {
		status = st.CannotOpenTableFiles
		return
	}
	defFileInfo, err := table.DefFile.Stat()
	if err != nil {
		status = st.CannotStatTableDefFile
		return
	}
	content := make([]byte, defFileInfo.Size)
	lines := strings.Split(string(content), "\n")
	table.NumberOfColumns = len(lines)
	for _, line := range lines {
		lengthName := strings.Split(line, ":")
		length, err := strconv.Atoi(lengthName[0])
		if err != nil {
			status = st.InvalidColumnDefinition
			return
		}
		aColumn := &column.Column{Offset:table.RowLength, Length:length, Name:lengthName[1]}
		table.Columns[lengthName[1]] = aColumn
		table.ColumnsInOrder = append(table.ColumnsInOrder[:], aColumn)
		table.RowLength += length
	}
	table.RowLength++
	status = st.OK
	return
}

func (table *Table) OpenFiles() (status int) {
	var err os.Error
	table.DefFile, err = os.OpenFile(table.DefFilePath, os.O_RDWR, constant.DataFilePerm)
	if err == nil {
		table.DataFile, err = os.OpenFile(table.DataFilePath, os.O_RDWR, constant.DataFilePerm)
		status = st.OK
		if err != nil {
			status = st.CannotOpenTableDataFile
		}
	} else {
		status = st.CannotOpenTableDefFile
	}
	return
}

func (table *Table) Flush() (status int){
	err := table.DefFile.Sync()
	if err == nil {
		err = table.DataFile.Sync()
		status = st.OK
		if err != nil {
			status = st.CannotFlushTableDataFile
		}
	} else {
		status = st.CannotFlushTableDefFile
	}
	return
}

func (table *Table) Seek(rowNumber int) (status int) {
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

func (table *Table) Add(name string, length int) (ok bool) {
	return
}

func (table *Table) RebuildDataFile() (ok bool) {
	return
}