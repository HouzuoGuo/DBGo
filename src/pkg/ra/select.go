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

/* Select *some* rows in a table of RA result according to a filter and a condition. */

package ra

import (
	"filter"
	"st"
)

// Relational algebra select.
func (r *Result) Select(alias string, filter filter.Filter, parameter interface{}) (*Result, int) {
	tableName := r.Aliases[alias].TableName
	columnName := r.Aliases[alias].ColumnName
	table := r.Tables[tableName].Table
	rowNumbers := r.Tables[tableName].RowNumbers
	kept := make([]int, 0)
	// Iterate through the rows of the table of RA result.
	for i := 0; i < len(rowNumbers); i++ {
		row, status := table.Read(rowNumbers[i])
		if status != st.OK {
			return r, status
		}
		// Keep the row if it passes the filter and is not a deleted row.
		if row["~del"] != "y" && filter.Cmp(row[columnName], parameter) {
			kept = append(kept[:], i)
		}
	}
	// Keep only the kept rows, for all existing tables of RA result.
	for _, table := range r.Tables {
		newRowNumbers := make([]int, len(kept))
		for i, keep := range kept {
			newRowNumbers[i] = table.RowNumbers[keep]
		}
		table.RowNumbers = newRowNumbers
	}
	return r, st.OK
}

// A condition for relational algebra select.
type Condition struct {
	Alias     string        // the column alias
	Filter    filter.Filter // filter function
	Parameter interface{}   // parameter to feed the filter function
}

// Same as relational algebra select but takes multiple conditions and run them one-by-one.
func (r *Result) MultipleSelect(conditions ...Condition) (*Result, int) {
	for _, condition := range conditions {
		_, status := r.Select(condition.Alias, condition.Filter, condition.Parameter)
		if status != st.OK {
			return r, status
		}
	}
	return r, st.OK
}
