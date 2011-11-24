/*
<DBGo - A flat-file relational database engine implementation in Go programming language>
Copyright (C) <2011>  <Houzuo (Howard) Guo>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

/*
DBGo table has:
tableName.data - table data, formatted like a spreadsheet, e.g.

yJOSHUA              FB                  CGG                                     
 NIKKI               MYB                 NH                                      
 BUZZ                TWITTER             BUZZ01                                  
 CHRISTINA           FACEBOOK            CG                                      
 CHRISTINA           SKYPE               JAMD

tableName.def - column definitions, e.g.

~del:1
NAME:20
SITE:20
USERNAME:40

Note that ~del is a special column, if ~del is set to "y", it means the row is deleted.

tableName.exclusive - when the table is exclusively locked by a transaction, the 
file is created and the content of the file is the ID of the transaction.

tableName.shared (directory) - when the table is locked by a transaction in shared mode, 
a file is created, the file name is the ID of the transaction.

This package handles basic, low-level table logics. 
*/

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
	"logg"
	"tablefilemanager"
)

type Table struct {
	// Path is the table's database's path, must end with /
	Path, Name, DefFilePath, DataFilePath string
	DefFile, DataFile                     *os.File
	Columns                               map[string]*column.Column
	RowLength                             int
	// sequence of columns
	ColumnsInOrder []*column.Column
}

// Opens a table.
func Open(path, name string) (*Table, int) {
	var table *Table
	table = new(Table)
	table.Path = path
	table.Name = name
	status := table.Init()
	if status != st.OK {
		logg.Err("table", "Open", "Failed to open"+path+name+" Err: "+string(status))
		return nil, status
	}
	return table, st.OK
}

// Load the table (column definitions, etc.).
func (table *Table) Init() int {
	// This function may be called multiple times, thus clear previous state.
	table.RowLength = 0
	table.Columns = make(map[string]*column.Column)
	table.ColumnsInOrder = make([]*column.Column, 0)
	table.DefFilePath = table.Path + table.Name + ".def"
	table.DataFilePath = table.Path + table.Name + ".data"
	status := table.OpenFiles()
	if status != st.OK {
		return status
	}
	defFileInfo, err := table.DefFile.Stat()
	if err != nil {
		logg.Err("table", "Init", err.String())
		return st.CannotStatTableDefFile
	}
	// Read definition file into memeory.
	content := make([]byte, defFileInfo.Size)
	table.DefFile.Read(content)
	// Each line contains one column definition.
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		if line != "" {
			var aColumn *column.Column
			// Convert the definition into a Column.
			aColumn, status = column.ColumnFromDef(table.RowLength, line)
			if status != st.OK {
				return status
			}
			table.Columns[aColumn.Name] = aColumn
			table.ColumnsInOrder = append(table.ColumnsInOrder[:], aColumn)
			table.RowLength += aColumn.Length
		}
	}
	table.RowLength++
	return st.OK
}

// Opens file handles.
func (table *Table) OpenFiles() int {
	var err os.Error
	table.DefFile, err = os.OpenFile(table.DefFilePath, os.O_RDWR, constant.DataFilePerm)
	if err == nil {
		table.DataFile, err = os.OpenFile(table.DataFilePath, os.O_RDWR, constant.DataFilePerm)
		if err != nil {
			logg.Err("table", "OpenFiles", err.String())
			return st.CannotOpenTableDataFile
		}
	} else {
		logg.Err("table", "OpenFiles", err.String())
		return st.CannotOpenTableDefFile
	}
	return st.OK
}

// Flushes table's files
func (table *Table) Flush() int {
	err := table.DefFile.Sync()
	if err == nil {
		err = table.DataFile.Sync()
		if err != nil {
			logg.Err("table", "Flush", err.String())
			return st.CannotFlushTableDataFile
		}
	} else {
		return st.CannotFlushTableDefFile
	}
	return st.OK
}

// Seeks to a row (e.g. row number 10).
func (table *Table) Seek(rowNumber int) int {
	var numberOfRows int
	numberOfRows, status := table.NumberOfRows()
	if status == st.OK && rowNumber < numberOfRows {
		_, err := table.DataFile.Seek(int64(rowNumber*table.RowLength), 0)
		if err != nil {
			logg.Err("table", "Seek", err.String())
			return st.CannotSeekTableDataFile
		}
	}
	return st.OK
}

// Seeks to a row and column (e.g. row number 10 column "NAME").
func (table *Table) SeekColumn(rowNumber int, columnName string) int {
	status := table.Seek(rowNumber)
	if status == st.OK {
		column, exists := table.Columns[columnName]
		if exists {
			_, err := table.DataFile.Seek(int64(column.Offset), 1)
			if err != nil {
				logg.Err("table", "SeekColumn", err.String())
				return st.CannotSeekTableDataFile
			}
		}
	}
	return st.OK
}

// Returns the number of rows in this table.
func (table *Table) NumberOfRows() (int, int) {
	var numberOfRows int
	var dataFileInfo *os.FileInfo
	dataFileInfo, err := table.DataFile.Stat()
	if err != nil {
		logg.Err("table", "NumberOfRows", err.String())
		return 0, st.CannotStatTableDataFile
	}
	numberOfRows = int(dataFileInfo.Size) / table.RowLength
	return numberOfRows, st.OK
}

// Reads a row and return a map representation (name1:value1, name2:value2...)
func (table *Table) Read(rowNumber int) (map[string]string, int) {
	row := make(map[string]string)
	status := table.Seek(rowNumber)
	if status == st.OK {
		rowInBytes := make([]byte, table.RowLength)
		_, err := table.DataFile.Read(rowInBytes)
		if err == nil {
			// For the columns in their order
			for _, column := range table.ColumnsInOrder {
				// column1:value2, column2:value2...
				row[column.Name] = strings.TrimSpace(string(rowInBytes[column.Offset : column.Offset+column.Length]))
			}
		} else {
			logg.Err("table", "Read", err.String())
			return nil, st.CannotReadTableDataFile
		}
	}
	return row, st.OK
}

// Writes a column value without seeking to a cursor position.
func (table *Table) Write(column *column.Column, value string) int {
	_, err := table.DataFile.WriteString(util.TrimLength(value, column.Length))
	if err != nil {
		return st.CannotWriteTableDataFile
	}
	return st.OK
}

// Inserts a row to the bottom of the table.
func (table *Table) Insert(row map[string]string) int {
	// Seek to EOF
	_, err := table.DataFile.Seek(0, 2)
	if err == nil {
		// For the columns in their order
		for _, column := range table.ColumnsInOrder {
			value, exists := row[column.Name]
			if !exists {
				value = ""
			}
			// Keep writing the column value.
			status := table.Write(column, value)
			if status != st.OK {
				return status
			}
		}
		// Write a new-line character.
		_, err = table.DataFile.WriteString("\n")
		if err != nil {
			logg.Err("table", "Insert", err.String())
			return st.CannotWriteTableDataFile
		}
	} else {
		logg.Err("table", "Insert", err.String())
		return st.CannotSeekTableDataFile
	}
	return st.OK
}

// Deletes a row.
func (table *Table) Delete(rowNumber int) int {
	status := table.Seek(rowNumber)
	if status == st.OK {
		del, exists := table.Columns["~del"]
		if exists {
			// Set ~del column value to "y" indicating the row is deleted
			return table.Write(del, "y")
		} else {
			return st.TableDoesNotHaveDelColumn
		}
	}
	return st.OK
}

// Updates a row.
func (table *Table) Update(rowNumber int, row map[string]string) int {
	for columnName, value := range row {
		column, exists := table.Columns[columnName]
		if exists {
			// Seek to the row and column, then write value in.
			status := table.SeekColumn(rowNumber, column.Name)
			if status != st.OK {
				return status
			}
			status = table.Write(column, value)
			if status != st.OK {
				return status
			}
		}
	}
	return st.OK
}

// Puts a new column.
func (table *Table) pushNewColumn(name string, length int) *column.Column {
	newColumn := &column.Column{Name: name, Offset: table.RowLength - 1, Length: length}
	table.ColumnsInOrder = append(table.ColumnsInOrder[:], newColumn)
	table.Columns[name] = newColumn
	return newColumn
}

// Adds a new column.
func (table *Table) Add(name string, length int) int {
	_, exists := table.Columns[name]
	if exists {
		return st.ColumnAlreadyExists
	}
	if len(name) > constant.MaxColumnNameLength {
		return st.ColumnNameTooLong
	}
	if length <= 0 {
		return st.InvalidColumnLength
	}
	var numberOfRows int
	numberOfRows, status := table.NumberOfRows()
	if status == st.OK && numberOfRows > 0 {
		// Rebuild data file if there are already rows in the table.
		// (To leave space for the new column)
		status = table.RebuildDataFile(name, length)
		table.pushNewColumn(name, length)
	} else {
		newColumn := table.pushNewColumn(name, length)
		// Write definition of the new column into definition file.
		_, err := table.DefFile.Seek(0, 2)
		if err != nil {
			logg.Err("table", "Add", err.String())
			return st.CannotSeekTableDefFile
		}
		_, err = table.DefFile.WriteString(column.ColumnToDef(newColumn))
		if err != nil {
			logg.Err("table", "Add", err.String())
			return st.CannotWriteTableDefFile
		}
	}
	table.RowLength += length
	return st.OK
}

// Removes a column.
func (table *Table) Remove(name string) int {
	var theColumn *column.Column
	// Find index of the column.
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
	if strings.HasPrefix(name, "~") {
		return st.CannotRemoveSpecialColumn
	}
	length := theColumn.Length
	// Remove the column from columns array.
	table.ColumnsInOrder = append(table.ColumnsInOrder[:columnIndex], table.ColumnsInOrder[columnIndex+1:]...)
	// Remove the column from columns map.
	table.Columns[name] = nil, true
	numberOfRows, status := table.NumberOfRows()
	if status != st.OK {
		return status
	}
	if numberOfRows > 0 {
		// Rebuild data file if there are already rows in the table.
		// (To remove data in the deleted column)
		status = table.RebuildDataFile("", 0)
	} else {
		status = util.RemoveLine(table.DefFilePath, column.ColumnToDef(theColumn))
	}
	table.RowLength -= length
	if status != st.OK {
		return status
	}
	return st.OK
}

// Rebuild data file, get rid off removed rows, optionally leaves space for a new column.
func (table *Table) RebuildDataFile(name string, length int) int {
	// Create a temporary table named by an accurate timestamp.
	tempName := strconv.Itoa64(time.Nanoseconds())
	tablefilemanager.Create(table.Path, tempName)
	var tempTable *Table
	tempTable, status := Open(table.Path, tempName)
	if status != st.OK {
		return status
	}
	// Put all columns of this table to the temporary table.
	for _, column := range table.ColumnsInOrder {
		tempTable.Add(column.Name, column.Length)
	}
	// Add the new column into the table as well.
	if name != "" {
		tempTable.Add(name, length)
	}
	var numberOfRows int
	numberOfRows, status = table.NumberOfRows()
	if status != st.OK {
		return status
	}
	var everFailed bool
	if name == "" {
		// If no new column, simply copy rows from this table to the temp table.
		for i := 0; i < numberOfRows; i++ {
			row, ret := table.Read(i)
			if ret != st.OK {
				everFailed = true
			}
			if row["~del"] != "y" {
				tempTable.Insert(row)
			}
		}
	} else {
		// If adding new column, not only copy rows from this table to the temporary one.
		// Also leave space for the new column's values.
		for i := 0; i < numberOfRows; i++ {
			row, ret := table.Read(i)
			if ret != st.OK {
				everFailed = true
			}
			if row["~del"] != "y" {
				row[name] = ""
				tempTable.Insert(row)
			}
		}
	}
	// Flush all the changes made to temporary table.
	status = tempTable.Flush()
	if everFailed || status != st.OK {
		return st.FailedToCopyCertainRows
	}
	// Delete the old table (one that is rebuilt), and rename the temporary 
	// table to the name of the rebuilt table.
	status = tablefilemanager.Delete(table.Path, table.Name)
	if status == st.OK {
		status = tablefilemanager.Rename(table.Path, tempName, table.Name)
		if status == st.OK {
			// Files have been changed, thus re-open file handles.
			return table.OpenFiles()
		}
	}
	return st.OK
}

// Returns an array of all rows, not including deleted rows.
func (table *Table) SelectAll() ([]map[string]string, int) {
	numberOfRows, status := table.NumberOfRows()
	if status != st.OK {
		return nil, status
	}
	var everFailed bool
	rows := make([]map[string]string, numberOfRows)
	for i := 0; i < numberOfRows; i++ {
		row, status := table.Read(i)
		if status != st.OK {
			everFailed = true
		}
		if row["~del"] != "y" {
			rows[i] = row
		}
	}
	if everFailed {
		return rows, st.FailedToReadCertainRows
	}
	return rows, st.OK
}
