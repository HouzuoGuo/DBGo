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

/* Manage table files, handles creation/renaming/removing of table files. */

package tablefilemanager

import (
	"os"
	"constant"
	"logg"
	"st"
)

// Creates table files.
func Create(path string, name string) int {
	if len(name) > constant.MaxTableNameLength {
		return st.TableNameTooLong
	}
	// Create table files with extension names.
	for _, ext := range constant.TableFiles() {
		_, err := os.Create(path + name + ext)
		if err != nil {
			logg.Err("tablefilemanager", "Create", err)
			return st.CannotCreateTableFile
		}
	}
	// Create table directories with name suffixes.
	for _, dir := range constant.TableDirs() {
		err := os.Mkdir(path+name+dir, constant.TableDirPerm)
		if err != nil {
			logg.Err("tablefilemanager", "Create", err)
			return st.CannotCreateTableDir
		}
	}
	return st.OK
}

// Renames table files.
func Rename(path string, oldName string, newName string) int {
	for _, ext := range constant.TableFiles() {
		err := os.Rename(path+oldName+ext, path+newName+ext)
		if err != nil {
			logg.Err("tablefilemanager", "Rename", err)
			return st.CannotRenameTableFile
		}
	}
	for _, dir := range constant.TableDirs() {
		err := os.Rename(path+oldName+dir, path+newName+dir)
		if err != nil {
			logg.Err("tablefilemanager", "Rename", err)
			return st.CannotRenameTableDir
		}
	}
	return st.OK
}

// Deletes table files
func Delete(path string, name string) int {
	for _, ext := range constant.TableFiles() {
		err := os.Remove(path + name + ext)
		if err != nil {
			logg.Err("tablefilemanager", "Delete", err)
			return st.CannotRemoveTableFile
		}
	}
	for _, dir := range constant.TableDirs() {
		err := os.RemoveAll(path + name + dir)
		if err != nil {
			logg.Err("tablefilemanager", "Delete", err)
			return st.CannotRemoveTableDir
		}
	}
	return st.OK
}
