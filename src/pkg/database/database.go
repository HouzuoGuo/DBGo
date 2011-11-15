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
func Open(path string) (db *Database, status int) {
	db = new(Database)
	db.Tables = make(map[string]*table.Table)
	// Open and read content of the path (as a directory).
	directory, err := os.Open(path)
	if err != nil {
		db = nil
		logg.Err("database", "Open", err.String())
		status = st.CannotOpenDatabaseDirectory
		return
	}
	defer directory.Close()
	fileInfo, err := directory.Readdir(0)
	if err != nil {
		db = nil
		logg.Err("database", "Open", err.String())
		status = st.CannotReadDatabaseDirectory
		return
	}
	for _, singleFileInfo := range fileInfo {
		// Extract extension of file name.
		if singleFileInfo.IsRegular() {
			name, ext := util.FilenameParts(singleFileInfo.Name)
			// If extension is .data, open the file as a Table.
			if ext == "data" {
				_, exists := db.Tables[name]
				if !exists {
					db.Tables[name], status = table.Open(path, name)
					if status != st.OK {
						db = nil
						return
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
func (db *Database) PrepareForTriggers(override bool) (status int) {
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
		return
	}
	afterTable, status := db.New("~after")
	if status != st.OK {
		return
	}
	// Prepare trigger lookup tables - add necessary columns.
	for _, t := range [...]*table.Table{beforeTable, afterTable} {
		for name, length := range constant.TriggerLookupTable() {
			status = t.Add(name, length)
			if status != st.OK {
				return
			}
		}
	}
	return
}

// Creates a new table.
func (db *Database) New(name string) (newTable *table.Table, status int) {
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
	newTable, status = table.Open(db.Path, name)
	if status == st.OK {
		// Add default columns
		for columnName, length := range constant.DatabaseColumns() {
			status = newTable.Add(columnName, length)
			if status != st.OK {
				return
			}
		}
		db.Tables[name] = newTable
	}
	return
}

// Removes a table.
func (db *Database) Remove(name string) (status int) {
	_, exists := db.Tables[name]
	if !exists {
		return st.TableNotFound
	}
	db.Tables[name] = nil, true
	// Remove table files and directories.
	return tablefilemanager.Delete(db.Path, name)
}

// Renames a table
func (db *Database) Rename(oldName, newName string) (status int) {
	_, exists := db.Tables[oldName]
	if !exists {
		return st.TableNotFound
	}
	_, exists = db.Tables[newName]
	if exists {
		return st.TableAlreadyExists
	}
	// Rename table files and directories
	status = tablefilemanager.Rename(db.Path, oldName, newName)
	db.Tables[newName] = db.Tables[oldName]
	db.Tables[oldName] = nil, true
	return st.OK
}

// Returns a Table by name.
func (db *Database) Get(name string) (table *table.Table, status int) {
	table, exists := db.Tables[name]
	if !exists {
		return nil, st.TableNotFound
	}
	return table, st.OK
}
