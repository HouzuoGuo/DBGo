package tablefilemanager

import (
	"os"
	"constant"
	"st"
)

func Create(path string, name string) (status int) {
	if len(name) > constant.MaxTableNameLength {
		status = st.TableNameTooLong
		return
	}
	for _, ext := range constant.TableFiles() {
		_, err := os.Create(path + name + ext)
		if err != nil {
			return st.CannotCreateTableFile
		}
	}
	for _, dir := range constant.TableDirs() {
		err := os.Mkdir(path + name + dir, constant.TableDirPerm)
		if err != nil {
			return st.CannotCreateTableDir
		}
	}
	return st.OK
}

func Rename(path string, oldName string, newName string) (status int) {
	for _, ext := range constant.TableFiles() {
		err := os.Rename(path + oldName + ext, path + newName + ext)
		if err != nil {
			return st.CannotRenameTableFile
		}
	}
	for _, dir := range constant.TableDirs() {
		err := os.Rename(path + oldName + dir, path + newName + dir)
		if err != nil {
			return st.CannotRenameTableDir
		}
	}
	return st.OK 
}

func Delete(path string, name string) (status int) {
	for _, ext := range constant.TableFiles() {
		err := os.Remove(path + name + ext)
		if err != nil {
			return st.CannotRemoveTableFile
		}
	}
	for _, dir := range constant.TableDirs() {
		err := os.RemoveAll(path + name + dir)
		if err != nil {
			return st.CannotRemoveTableDir
		}
	}
	return st.OK
}