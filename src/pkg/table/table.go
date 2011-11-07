package table

import (
	"os"
	"time"
	"strings"
	"strconv"
	"column"
	"constant"
	"st"
	"util"
	"tablefilemanager"
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
	if status != st.OK {
		table = nil
	}
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
		var aColumn *column.Column
		aColumn, status = column.ColumnFromDef(table.RowLength, line)
		if status != st.OK {
			return
		}
		table.Columns[aColumn.Name] = aColumn
		table.ColumnsInOrder = append(table.ColumnsInOrder[:], aColumn)
		table.RowLength += aColumn.Length
	}
	table.RowLength++
	return
}

func (table *Table) OpenFiles() (status int) {
	var err os.Error
	table.DefFile, err = os.OpenFile(table.DefFilePath, os.O_RDWR, constant.DataFilePerm)
	if err == nil {
		table.DataFile, err = os.OpenFile(table.DataFilePath, os.O_RDWR, constant.DataFilePerm)
		if err != nil {
			return st.CannotOpenTableDataFile
		}
	} else {
		return st.CannotOpenTableDefFile
	}
	return st.OK
}

func (table *Table) Flush() (status int){
	err := table.DefFile.Sync()
	if err == nil {
		err = table.DataFile.Sync()
		if err != nil {
			return st.CannotFlushTableDataFile
		}
	} else {
		return st.CannotFlushTableDefFile
	}
	return st.OK
}

func (table *Table) Seek(rowNumber int) (status int) {
	var numberOfRows int
	numberOfRows, status = table.NumberOfRows()
	if status == st.OK && rowNumber < numberOfRows {
		_, err := table.DataFile.Seek(int64(rowNumber * table.RowLength), 0)
		if err != nil {
			return st.CannotSeekTableDataFile
		}
	}
	return st.OK
}

func (table *Table) SeekColumn(rowNumber int, columnName string) (status int) {
	status = table.Seek(rowNumber)
	if status == st.OK {
		column, exists := table.Columns[columnName]
		if exists {
			_, err := table.DataFile.Seek(int64(column.Offset), 1)
			if err != nil {
				return st.CannotSeekTableDataFile
			}
		}
	}
	return st.OK
}

func (table *Table) NumberOfRows() (numberOfRows int, status int) {
	var dataFileInfo *os.FileInfo
	dataFileInfo, err := table.DataFile.Stat()
	if err == nil {
		numberOfRows = int(dataFileInfo.Size)
		status = st.OK
	} else {
		status = st.CannotStatTableDataFile;
	}
	return
}

func (table *Table) Read(rowNumber int) (row map[string]string, status int) {
	status = table.Seek(rowNumber)
	if status == st.OK {
		rowInBytes := make([]byte, table.NumberOfColumns)
		_, err := table.DataFile.Read(rowInBytes)
		if err == nil {
			for _, column := range table.ColumnsInOrder {
				row[column.Name] = strings.TrimSpace(string(rowInBytes[column.Offset:column.Offset+column.Length]))
			}
			status = st.OK
		} else {
			status = st.CannotReadTableDataFile
		}
	}
	return
}

func (table *Table) Write(column *column.Column, value string) (status int){
	_, err := table.DataFile.WriteString(util.TrimLength(value, column.Length))
	if err != nil {
		return st.CannotWriteTableDataFile
	}
	return st.OK
}

func (table *Table) Insert(row map[string]string) (status int) {
	_, err := table.DataFile.Seek(0, 2)
	if err == nil {
		for _, column := range table.ColumnsInOrder {
			value, exists := row[column.Name]
			if !exists {
				value = ""
			} 
			status = table.Write(column, value)
			if status != st.OK {
				return
			}
		}
		_, err = table.DataFile.WriteString("\n")
		if err == nil {
			return st.CannotWriteTableDataFile
		}
	}
	return st.OK
}

func (table *Table) Delete(rowNumber int) (status int) {
	status = table.Seek(rowNumber)
	if status == st.OK {
		del, exists := table.Columns["~del"]
		if exists {
			status = table.Write(del, "y") 
		} else {
			status = st.TableDoesNotHaveDelColumn
		}
	}
	return
}

func (table *Table) Update(rowNumber int, row map[string]string) (status int) {
	status = table.Seek(rowNumber)
	if status == st.OK {
		for columnName, value := range row {
			column, exists := table.Columns[columnName]
			if exists {
				status = table.Write(column, value)
				if status != st.OK {
					return
				}
			} else {
				return
			}
		}
	}
	return
}

func (table *Table) pushNewColumn(name string, length int) *column.Column{
	newColumn := &column.Column{Name:name, Length:length}
	table.ColumnsInOrder = append(table.ColumnsInOrder[:], newColumn)
	table.Columns[name] = newColumn
	return newColumn
}

func (table *Table) Add(name string, length int) (status int) {
	_, exists := table.Columns[name]
	if exists {
		return st.ColumnAlreadyExists
	}
	if len(name) > constant.MaxColumnNameLength {
		return st.ColumnNameTooLong
	}
	newColumn := table.pushNewColumn(name, length)
	var numberOfRows int
	numberOfRows, status = table.NumberOfRows()
	if status == st.OK && numberOfRows > 0 {
		newColumn.Offset = 0
		newColumn.Length = 1
		status = table.RebuildDataFile()
		newColumn.Offset = table.RowLength
		newColumn.Length = length
	}
	if status == st.OK {
		_, err := table.DefFile.Seek(0, 2)
		if err != nil {
			return st.CannotSeekTableDefFile
		}
		_, err = table.DefFile.WriteString(column.ColumnToDef(newColumn))
		if err != nil {
			return st.CannotWriteTableDefFile
		}
		table.RowLength += length
	}
}

func (table *Table) Remove(name string) (status int) {
	var theColumn *column.Column
	var columnIndex int
	for i, column := range table.ColumnsInOrder {
		if column.Name == name {
			theColumn = table.ColumnsInOrder[i]
			columnIndex = i
			break 
		}
	}
	if theColumn == nil {
		return st.ColumnNameNotFound
	}
	name, length := theColumn.Name, theColumn.Length
	table.ColumnsInOrder = append(table.ColumnsInOrder[:columnIndex], table.ColumnsInOrder[columnIndex:]...)
	table.Columns[name] = nil, true
	table.RebuildDataFile()
	table.RowLength -= length
	
}

func (table *Table) RebuildDataFile() (status int) {
	tempName := string(time.Nanoseconds())
	tablefilemanager.Create(table.Path, tempName)
	var tempTable *Table
	tempTable, status = Open(table.Path, tempName)
	if status != st.OK {
		return
	}
	for _, column := range table.ColumnsInOrder {
		tempTable.Add(column.Name, column.Length)
	}
	var numberOfRows int
	numberOfRows, status = table.NumberOfRows()
	if status != st.OK {
		return
	}
	var everFailed bool
	for i := 0; i < numberOfRows; i ++ {
		row, ret := table.Read(i)
		if ret != st.OK {
			everFailed = true
		}
		if row["~del"] != "y" {
			tempTable.Insert(row)
		}
	}
	status = tempTable.Flush()
	if everFailed || status != st.OK {
		status = st.FailedToCopyCertainRows
		return
	}
	status = tablefilemanager.Delete(table.Path, table.Name)
	if status == st.OK {
		status = tablefilemanager.Rename(table.Path, tempName, table.Name)
		if status == st.OK {
			return table.OpenFiles()
		}
	}
	return
}