// Database logics, create/rename/remove tables, etc.

package database

import (
	"os"
	"table"
	"util"
	"st"
	"constant"
	"logg"
	"tablefilemanager"
)

type Database struct {
	Path   string
	Tables map[string]*table.Table
}

// Opens a path as database.
func Open(path string) (*Database, int) {
	var db *Database
	db = new(Database)
	db.Tables = make(map[string]*table.Table)
	// Open and read content of the path (as a directory).
	directory, err := os.Open(path)
	if err != nil {
		db = nil
		logg.Err("database", "Open", err.String())
		return db, st.CannotOpenDatabaseDirectory
	}
	defer directory.Close()
	fileInfo, err := directory.Readdir(0)
	if err != nil {
		db = nil
		logg.Err("database", "Open", err.String())
		return db, st.CannotReadDatabaseDirectory
	}
	for _, singleFileInfo := range fileInfo {
		// Extract extension of file name.
		if singleFileInfo.IsRegular() {
			name, ext := util.FilenameParts(singleFileInfo.Name)
			// If extension is .data, open the file as a Table.
			if ext == "data" {
				_, exists := db.Tables[name]
				if !exists {
					var status int
					db.Tables[name], status = table.Open(path, name)
					if status != st.OK {
						return nil, status
					}
				}
			}
		}
	}
	db.Path = path
	return db, db.PrepareForTriggers(false)
}

// Prepare the database for using table triggers.
// If override is set to true, remove all existing table triggers and re-create trigger lookup tables.
func (db *Database) PrepareForTriggers(override bool) int {
	if override {
		// Remove all existing table triggers.
		db.Remove("~before")
		db.Remove("~after")
	} else {
		// If .init file exists, no need to redo the process.
		_, err := os.Open(db.Path + ".init")
		if err == nil {
			return st.OK
		}
	}
	// Create flag file .init.
	_, err := os.OpenFile(db.Path+".init", os.O_CREATE, constant.InitFilePerm)
	if err != nil {
		return st.CannotCreateInitFile
	}
	// Create ~before ("before" triggers) and ~after ("after" triggers) tables.
	beforeTable, status := db.New("~before")
	if status != st.OK {
		return status
	}
	afterTable, status := db.New("~after")
	if status != st.OK {
		return status
	}
	// Prepare trigger lookup tables - add necessary columns.
	for _, t := range [...]*table.Table{beforeTable, afterTable} {
		for name, length := range constant.TriggerLookupTable() {
			status = t.Add(name, length)
			if status != st.OK {
				return status
			}
		}
	}
	return st.OK
}

// Creates a new table.
func (db *Database) New(name string) (*table.Table, int) {
	var newTable *table.Table
	_, exists := db.Tables[name]
	if exists {
		return nil, st.TableAlreadyExists
	}
	if len(name) > constant.MaxTableNameLength {
		return nil, st.TableNameTooLong
	}
	// Create files and directories.
	tablefilemanager.Create(db.Path, name)
	// Open the table
	var status int
	newTable, status = table.Open(db.Path, name)
	if status == st.OK {
		// Add default columns
		for columnName, length := range constant.DatabaseColumns() {
			status = newTable.Add(columnName, length)
			if status != st.OK {
				return nil, status
			}
		}
		db.Tables[name] = newTable
	}
	return newTable, st.OK
}

// Removes a table.
func (db *Database) Remove(name string) int {
	_, exists := db.Tables[name]
	if !exists {
		return st.TableNotFound
	}
	db.Tables[name] = nil, true
	// Remove table files and directories.
	return tablefilemanager.Delete(db.Path, name)
}

// Renames a table
func (db *Database) Rename(oldName, newName string) int {
	_, exists := db.Tables[oldName]
	if !exists {
		return st.TableNotFound
	}
	_, exists = db.Tables[newName]
	if exists {
		return st.TableAlreadyExists
	}
	// Rename table files and directories
	status := tablefilemanager.Rename(db.Path, oldName, newName)
	if status != st.OK {
		return status
	}
	db.Tables[newName] = db.Tables[oldName]
	db.Tables[oldName] = nil, true
	return st.OK
}

// Returns a Table by name.
func (db *Database) Get(name string) (*table.Table, int) {
	var table *table.Table
	table, exists := db.Tables[name]
	if !exists {
		return nil, st.TableNotFound
	}
	return table, st.OK
}
