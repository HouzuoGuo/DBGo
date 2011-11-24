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

/* Some utility functions for file handling. */

package util

import (
	"os"
	"strings"
	"st"
	"logg"
)

// Creates a file and writes the content into it.
func CreateAndWrite(filename, content string) int {
	file, err := os.Create(filename)
	defer file.Close()
	if err != nil {
		logg.Err("util", "CreateAndWrite", err)
		return st.CannotCreateFile
	}
	_, err = file.WriteString(content)
	if err != nil {
		logg.Err("util", "CreateAndWrite", err)
		return st.CannotCreateFile
	}
	return st.OK
}

// Removes a line's occurances from a file.
func RemoveLine(filename, line string) int {
	// Open and read the file.
	file, err := os.Open(filename)
	if err != nil {
		logg.Err("util", "RemoveLine", err)
		return st.CannotReadFile
	}
	fi, err := file.Stat()
	if err != nil {
		logg.Err("util", "RemoveLine", err)
		return st.CannotReadFile
	}
	buffer := make([]byte, fi.Size)
	_, err = file.Read(buffer)
	if err != nil {
		logg.Err("util", "RemoveLine", err)
		return st.CannotReadFile
	}
	file.Close()
	// Re-open the file and overwrite it.
	file, err = os.OpenFile(filename, os.O_WRONLY+os.O_TRUNC, 0666)
	defer file.Close()
	if err != nil {
		logg.Err("util", "RemoveLine", err)
		return st.CannotReadFile
	}
	lines := strings.Split(string(buffer), "\n")
	for _, content := range lines {
		if strings.TrimSpace(content) != strings.TrimSpace(line) {
			_, err = file.WriteString(content + "\n")
			if err != nil {
				logg.Err("util", "RemoveLine", err)
				return st.CannotWriteFile
			}
		}
	}
	return st.OK
}
