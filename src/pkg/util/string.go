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

/* Some utility functions for string handling. */

package util

import (
	"strings"
)

// Returns a string which is the original string trimmed to the desired length.
// Trailing spaces are added if the string's length is too short.
// Otherwise, the string is truncated from right to the desired length.
func TrimLength(str string, length int) (trimmed string) {
	lengthDiff := length - len(str)
	if lengthDiff > 0 {
		trimmed = str + strings.Repeat(" ", lengthDiff)
	} else {
		trimmed = str[:length]
	}
	return trimmed
}

// Returns file name (without extension) and extension of a file name.
func FilenameParts(filename string) (name, extension string) {
	dotIndex := strings.LastIndex(filename, ".")
	if dotIndex == -1 || dotIndex == len(filename)-1 {
		name = filename
		extension = ""
	} else {
		name = filename[:dotIndex]
		extension = filename[dotIndex+1:]
	}
	return
}
