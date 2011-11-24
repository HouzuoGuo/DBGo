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

/* Join a table in RA result with another table using nested loops. */

package ra

import (
	"table"
	"st"
)

// Relational algebra join using nested loops.
func (r *Result) NLJoin(alias string, t2 *table.Table, name string) (*Result, int) {
	// t1 is the table in RA result.
	t1Column := r.Aliases[alias].ColumnName
	t1 := r.Tables[r.Aliases[alias].TableName]
	t2RowNumbers := make([]int, 0)
	// t2 is the external table.
	t2NumberOfRows, status := t2.NumberOfRows()
	if status != st.OK {
		return r, status
	}
	// Prepare to re-arrange the sequence of row numbers of all existing tables in RA result.
	newRowNumbers := make(map[string][]int)
	for name, _ := range r.Tables {
		newRowNumbers[name] = make([]int, 0)
	}
	// NL begins.
	for i, t1RowNumber := range t1.RowNumbers {
		for t2RowNumber := 0; t2RowNumber < t2NumberOfRows; t2RowNumber++ {
			t1Row, status := t1.Table.Read(t1RowNumber)
			if status != st.OK {
				return r, status
			}
			t2Row, status := t2.Read(t2RowNumber)
			if status != st.OK {
				return r, status
			}
			if t1Row["~del"] != "y" && t2Row["~del"] != "y" && t1Row[t1Column] == t2Row[name] {
				for name, _ := range newRowNumbers {
					newRowNumbers[name] = append(newRowNumbers[name][:], r.Tables[name].RowNumbers[i])
				}
				t2RowNumbers = append(t2RowNumbers[:], t2RowNumber)
			}
		}
	}
	// Re-arrange the sequence of row numbers of all existing tables in RA result.
	for name, rowNumbers := range newRowNumbers {
		r.Tables[name].RowNumbers = rowNumbers
	}
	// Load columns of t2 into RA result.
	r.Load(t2)
	t2Table := r.Tables[t2.Name]
	t2Table.RowNumbers = t2RowNumbers
	return r, st.OK
}
