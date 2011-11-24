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
Database logics.

DBGo database is stored in a directory. DBGo data files do not use very special extension names,
thus it is better to give a DBGo database an empty directory to begin with, and better not to 
store any other user files in the directory.
*/

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
	Path   string // path to database directory, must end with slash /
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
	fi, err := directory.Readdir(0)
	if err != nil {
		db = nil
		logg.Err("database", "Open", err.String())
		return db, st.CannotReadDatabaseDirectory
	}
	for _, fileInfo := range fi {
		// Extract extension of file name.
		if fileInfo.IsRegular() {
			name, ext := util.FilenameParts(fileInfo.Name)
			// If extension is .data, open the file as a Table.
			if ext == "data" {
				_, exists := db.Tables[name]
				if !exists {
					var status int
					// Open the table and put it into tables map.
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
// If override is set to true, it will remove all existing table triggers and re-create trigger lookup tables.
func (db *Database) PrepareForTriggers(override bool) int {
	if override {
		// Remove all existing table triggers.
		db.Drop("~before")
		db.Drop("~after")
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
	beforeTable, status := db.Create("~before")
	if status != st.OK {
		return status
	}
	afterTable, status := db.Create("~after")
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
func (db *Database) Create(name string) (*table.Table, int) {
	var newTable *table.Table
	_, exists := db.Tables[name]
	if exists {
		return nil, st.TableAlreadyExists
	}
	if len(name) > constant.MaxTableNameLength {
		return nil, st.TableNameTooLong
	}
	// Create table files and directories.
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

// Drops a table.
func (db *Database) Drop(name string) int {
	_, exists := db.Tables[name]
	if !exists {
		return st.TableNotFound
	}
	db.Tables[name] = nil, false
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
	db.Tables[oldName].Flush()
	// Rename table files and directories
	status := tablefilemanager.Rename(db.Path, oldName, newName)
	if status != st.OK {
		return status
	}
	db.Tables[newName], status = table.Open(db.Path, newName)
	db.Tables[oldName] = nil, false
	return status
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

// Flushes all tables.
func (db *Database) Flush() {
	for _, t := range db.Tables {
		t.Flush()
	}
}
