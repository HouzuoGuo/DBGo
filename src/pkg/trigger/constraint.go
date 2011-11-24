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

/* Common trigger functions related to table constraints. */

package trigger

import (
	"table"
	"database"
	"st"
)

// Look for a value in a table's column, returns true if the value is found. 
func find(column, value string, t *table.Table) (bool, int) {
	numberOfRows, status := t.NumberOfRows()
	if status != st.OK {
		return false, status
	}
	for i := 0; i < numberOfRows; i++ {
		row, status := t.Read(i)
		if status != st.OK {
			return false, status
		}
		if row[column] == value {
			return true, st.OK
		}
	}
	return false, st.OK
}

// Primary key constraint
type PK struct {
	TriggerFunc
}

func (pk PK) Execute(db *database.Database, t *table.Table, column string, extraParameters []string, row1, row2 map[string]string) int {
	found, status := find(column, row1[column], t)
	if found && status == st.OK {
		return st.DuplicatedPKValue
	}
	return status
}

// Foreign key constraint
type FK struct {
	TriggerFunc
}

func (fk FK) Execute(db *database.Database, t *table.Table, column string, extraParameters []string, row1, row2 map[string]string) int {
	// extraParameters is PK table name[0] and PK column name[1]
	pkTable, status := db.Get(extraParameters[0])
	if status != st.OK {
		return status
	}
	found, status := find(extraParameters[1], row1[column], pkTable)
	if !found && status == st.OK {
		return st.InvalidFKValue
	}
	return status
}

// Delete Restricted
type DR struct {
	TriggerFunc
}

func (dr DR) Execute(db *database.Database, t *table.Table, column string, extraParameters []string, row1, row2 map[string]string) int {
	// extraParameters is FK table name[0] and FK column name[1]
	fkTable, status := db.Get(extraParameters[0])
	if status != st.OK {
		return status
	}
	found, status := find(extraParameters[1], row1[column], fkTable)
	if found && status == st.OK {
		return st.DeleteRestricted
	}
	return status
}

// Update Restricted
type UR struct {
	TriggerFunc
}

func (ur UR) Execute(db *database.Database, t *table.Table, column string, extraParameters []string, row1, row2 map[string]string) int {
	// extraParameters is FK table name[0] and FK column name[1]
	fkTable, status := db.Get(extraParameters[0])
	if status != st.OK {
		return status
	}
	found, status := find(extraParameters[1], row2[column], fkTable)
	if found && status == st.OK {
		return st.UpdateRestricted
	}
	return status
}
