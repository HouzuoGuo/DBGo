package tablefilemanager

import (
	"os"
	"constant"
	"st"
)

func Create(path string, name string) (status int) {
	defFilePath := path + name + ".def"
	dataFilePath := path + name + ".data"
	if len(name) > constant.MaxTableNameLength {
		status = st.TableNameTooLong
		return
	}
	_, err := os.Create(defFilePath)
	if err != nil {
		status = st.CannotCreateTableDefFile
		return
	}
	_, err = os.Create(dataFilePath)
	status = st.OK
	if err != nil {
		status = st.CannotCreateTableDataFile
	}
	return
}

func Rename(path string, oldName string, newName string) (status int) {
	oldDefFilePath := path + oldName + ".def"
	newDefFilePath := path + newName + ".def"
	oldDataFilePath := path + oldName + ".data"
	newDataFilePath := path + newName + ".data"
	err := os.Rename(oldDefFilePath, newDefFilePath)
	if err != nil {
		status = st.CannotRenameTableDefFile
		return
	}
	err = os.Rename(oldDataFilePath, newDataFilePath)
	status = st.OK
	if err != nil {
		status = st.CannotRenameTableDataFile
	}
	return 
}

func Delete(path string, name string) (status int) {
	return 0
}