package database

import (
	"os"
	"table"
	"util"
	"st"
)

type Database struct {
	Path string
	Tables map[string]*table.Table
}

func Open(path string) (db *Database, status int) {
	db = new(Database)
	var directory *os.File
	var err os.Error
	directory, err = os.Open(path)
	if err != nil {
		db = nil
		status = st.CannotOpenDatabaseDirectory
		return
	}
	defer directory.Close()
	var fileInfo []os.FileInfo
	fileInfo, err = directory.Readdir(0)
	if err != nil {
		db = nil
		status = st.CannotReadDatabaseDirectory
		return
	}
	for _, singleFileInfo := range fileInfo {
		if singleFileInfo.IsRegular() {
			name, extension := util.FilenameParts(singleFileInfo.Name)
			switch extension {
				case ".data":
					fallthrough
				case ".def":
					fallthrough
				case ".log":
					_, exists := db.Tables[name]
					if !exists {
						var ret int
						db.Tables[name], ret = table.Open(path, name)
						if ret != st.OK {
							db = nil
							status = st.CannotOpenTableInDatabaseInit
							return
						}
					}
			}
		}
	}
	db.Path = path
	status = st.OK
	return
}

func (db *Database) New (tableName string) (table *table.Table, status int) {
	return
}

func (db *Database) Delete (tableName string) (table *table.Table, status int) {
	return
}