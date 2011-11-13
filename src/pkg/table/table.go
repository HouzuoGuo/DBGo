// Table logics, add/delete/update rows, add/remove columns, etc.

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
	Path, Name, DefFilePath, DataFilePath string
	DefFile, DataFile                     *os.File
	Columns                               map[string]*column.Column
	RowLength                             int
	ColumnsInOrder                        []*column.Column
}

// Opens a table.
func Open(path, name string) (table *Table, status int) {
	table = new(Table)
	table.Path = path
	table.Name = name
	status = table.Init()
	if status != st.OK {
		logg.Err("table", "Open", "Failed to open"+path+name+" Err: "+string(status))
		table = nil
	}
	return
}

// Load the table (column definitions, etc.).
func (table *Table) Init() (status int) {
	// This function may be called multiple times, thus clear previous state.
	table.RowLength = 0
	table.Columns = make(map[string]*column.Column)
	table.ColumnsInOrder = make([]*column.Column, 0)
	table.DefFilePath = table.Path + table.Name + ".def"
	table.DataFilePath = table.Path + table.Name + ".data"
	ret := table.OpenFiles()
	if ret != st.OK {
		return
	}
	defFileInfo, err := table.DefFile.Stat()
	if err != nil {
		logg.Err("table", "Init", err.String())
		status = st.CannotStatTableDefFile
		return
	}
	// Read definition file into memeory.
	content := make([]byte, defFileInfo.Size)
	table.DefFile.Read(content)
	// Each line contains one column definition
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		if line != "" {
			var aColumn *column.Column
			// Convert the definition into a Column
			aColumn, status = column.ColumnFromDef(table.RowLength, line)
			if status != st.OK {
				return
			}
			table.Columns[aColumn.Name] = aColumn
			table.ColumnsInOrder = append(table.ColumnsInOrder[:], aColumn)
			table.RowLength += aColumn.Length
		}
	}
	table.RowLength++
	return
}

// Opens file handles.
func (table *Table) OpenFiles() (status int) {
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
func (table *Table) Flush() (status int) {
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
func (table *Table) Seek(rowNumber int) (status int) {
	var numberOfRows int
	numberOfRows, status = table.NumberOfRows()
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
func (table *Table) SeekColumn(rowNumber int, columnName string) (status int) {
	status = table.Seek(rowNumber)
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
func (table *Table) NumberOfRows() (numberOfRows int, status int) {
	var dataFileInfo *os.FileInfo
	dataFileInfo, err := table.DataFile.Stat()
	if err == nil {
		numberOfRows = int(dataFileInfo.Size) / table.RowLength
		status = st.OK
	} else {
		logg.Err("table", "NumberOfRows", err.String())
		status = st.CannotStatTableDataFile
	}
	return
}

// Reads a row and return a map representation (name1:value1, name2:value2...)
func (table *Table) Read(rowNumber int) (row map[string]string, status int) {
	row = make(map[string]string)
	status = table.Seek(rowNumber)
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
			status = st.CannotReadTableDataFile
		}
	}
	return
}

// Writes a column value without seeking to a cursor position.
func (table *Table) Write(column *column.Column, value string) (status int) {
	_, err := table.DataFile.WriteString(util.TrimLength(value, column.Length))
	if err != nil {
		return st.CannotWriteTableDataFile
	}
	return st.OK
}

// Inserts a row to the bottom of the table.
func (table *Table) Insert(row map[string]string) (status int) {
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
			status = table.Write(column, value)
			if status != st.OK {
				return
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
func (table *Table) Delete(rowNumber int) (status int) {
	status = table.Seek(rowNumber)
	if status == st.OK {
		del, exists := table.Columns["~del"]
		if exists {
			// Set ~del column value to "y" indicating the row is deleted
			status = table.Write(del, "y")
		} else {
			status = st.TableDoesNotHaveDelColumn
		}
	}
	return
}

// Updates a row.
func (table *Table) Update(rowNumber int, row map[string]string) (status int) {
	for columnName, value := range row {
		column, exists := table.Columns[columnName]
		if exists {
			// Seek to the row and column, then write value in.
			status = table.SeekColumn(rowNumber, column.Name)
			if status != st.OK {
				return
			}
			status = table.Write(column, value)
			if status != st.OK {
				return
			}
		}
	}
	return
}

// Puts a new column in the Table struct.
func (table *Table) pushNewColumn(name string, length int) *column.Column {
	newColumn := &column.Column{Name: name, Offset: table.RowLength - 1, Length: length}
	table.ColumnsInOrder = append(table.ColumnsInOrder[:], newColumn)
	table.Columns[name] = newColumn
	return newColumn
}

// Adds a new column.
func (table *Table) Add(name string, length int) (status int) {
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
	numberOfRows, status = table.NumberOfRows()
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
	return
}

// Removes a column.
func (table *Table) Remove(name string) (status int) {
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
	name, length := theColumn.Name, theColumn.Length
	// Remove the column from columns array.
	table.ColumnsInOrder = append(table.ColumnsInOrder[:columnIndex], table.ColumnsInOrder[columnIndex+1:]...)
	// Remove the column from columns map.
	table.Columns[name] = nil, true
	numberOfRows, status := table.NumberOfRows()
	if status != st.OK {
		return
	}
	if numberOfRows > 0 {
		// Rebuild data file if there are already rows in the table.
		// (To remove data in the deleted column)
		status = table.RebuildDataFile("", 0)
	}
	table.RowLength -= length
	return
}

// Rebuild data file, get rid off removed rows, optionally leaves space for a 
// new column.
func (table *Table) RebuildDataFile(name string, length int) (status int) {
	// Create a temporary table named by an accurate timestamp.
	tempName := strconv.Itoa64(time.Nanoseconds())
	tablefilemanager.Create(table.Path, tempName)
	var tempTable *Table
	tempTable, status = Open(table.Path, tempName)
	if status != st.OK {
		return
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
		return
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
		// If adding new column, not only copy rows from this table to the temp 
		// one, also leave space for the new column's values.
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
		status = st.FailedToCopyCertainRows
		return
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
	return
}
