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
Conversions between text definition of a column and a Column.

A column is defined in the following format in table's ".def" file:
columnName1:maxLength1
columnName2:maxLength2
columnName3:maxLength3
*/

package column

import (
	"strconv"
	"strings"
	"st"
	"logg"
)

type Column struct {
	Offset int // offset of the column in table row
	Length int // max length of the column
	Name   string
}

// Constructs a Column from a column's text definition.
func ColumnFromDef(offset int, definition string) (*Column, int) {
	var column *Column
	// Extract length and name from the definition.
	lengthName := strings.Split(definition, ":")
	length, err := strconv.Atoi(lengthName[1])
	if err != nil {
		logg.Err("Column", "ColumnFromDef", "Definition malformed: "+definition)
		return nil, st.InvalidColumnDefinition
	}
	column = &Column{Offset: offset, Length: length, Name: lengthName[0]}
	return column, st.OK
}

// Constructs a text definition of a column.
func ColumnToDef(column *Column) string {
	return column.Name + ":" + strconv.Itoa(column.Length) + "\n"
}

// <The Bible Code> is a very interesting book :)
