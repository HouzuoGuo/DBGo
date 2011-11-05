package database

import (
	"os"
	"table"
	"util"
)

type Database struct {
	path string
	tables map[string]*table.Table
}

func OpenDatabase(path string) (db *Database) {
	var directory *os.File
	var error os.Error
	directory, error = os.Open(path)
	if error != nil {
		db = nil
	}
	defer directory.Close()
	var fileInfo []os.FileInfo
	fileInfo, error = directory.Readdir(0)
	if error != nil {
		db = nil
	}
	for i := 0; i< len(fileInfo); i++ {
		if !fileInfo[i].IsRegular() {
			continue
		}
		name, extension := util.FilenameParts(fileInfo[i].Name)
		switch extension {
			case ".data":
				fallthrough
			case ".def":
				fallthrough
			case ".log":
				_, exists := db.tables[name]
				if !exists {
					db.tables[name] = table.OpenTable(path, name)
				}
		}
	}
	return
}