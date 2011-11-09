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
	Path string
	Tables map[string]*table.Table
}

func Open(path string) (db *Database, status int) {
	db = new(Database)
	db.Tables = make(map[string]*table.Table)
	var directory *os.File
	var err os.Error
	directory, err = os.Open(path)
	if err != nil {
		db = nil
		logg.Err("database", "Open", err.String())
		status = st.CannotOpenDatabaseDirectory
		return
	}
	defer directory.Close()
	var fileInfo []os.FileInfo
	fileInfo, err = directory.Readdir(0)
	if err != nil {
		db = nil
		logg.Err("database", "Open", err.String())
		status = st.CannotReadDatabaseDirectory
		return
	}
	for _, singleFileInfo := range fileInfo {
		if singleFileInfo.IsRegular() {
			name, ext := util.FilenameParts(singleFileInfo.Name)
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
	return
}

func (db *Database) New (name string) (newTable *table.Table, status int) {
	_, exists := db.Tables[name]
	if exists {
		return nil, st.TableAlreadyExists
	}
	if len(name) > constant.MaxTableNameLength {
		return nil, st.TableNameTooLong
	}
	tablefilemanager.Create(db.Path, name)
	newTable, status = table.Open(db.Path, name)
	if status == st.OK {
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

func (db *Database) Remove (name string) (status int) {
	_, exists := db.Tables[name]
	if !exists {
		return st.TableNotFound
	}
	db.Tables[name] = nil, true
	return tablefilemanager.Delete(db.Path, name)
}

func (db *Database) Get (name string) (table *table.Table) {
	table, _ = db.Tables[name]
	return
}