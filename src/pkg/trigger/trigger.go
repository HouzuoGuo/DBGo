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

/* Execute approrpiate trigger functions according to table operation. */

package trigger

import (
	"strings"
	"table"
	"ra"
	"filter"
	"database"
	"st"
)

// Trigger function must implement this interface.
type TriggerFunc interface {
	Execute(db *database.Database, t *table.Table, column string, extraParameters []string, row1, row2 map[string]string) int
}

// Returns a map of trigger function names and trigger body structs.
// All trigger functions mentioned in trigger lookup table must be registered here.
func TriggerFuncTable() map[string]TriggerFunc {
	return map[string]TriggerFunc{"PK": PK{}, "FK": FK{}, "UR": UR{}, "DR": DR{}}
}

// Executes triggers according to the table operation.
func ExecuteTrigger(db *database.Database, t *table.Table, r *ra.Result, operation string, row1, row2 map[string]string) int {
	for column, _ := range row1 {
		raCopy := r.Copy()
		// Filter according to the column name and operation type.
		raCopy.MultipleSelect(ra.Condition{Alias: "COLUMN", Filter: filter.Eq{}, Parameter: column},
			ra.Condition{Alias: "OP", Filter: filter.Eq{}, Parameter: operation})
		// For each trigger.
		for i := 0; i < raCopy.NumberOfRows(); i++ {
			row, status := raCopy.Read(i)
			if status != st.OK {
				return status
			}
			/*
				Call the trigger function. Parameters given are:
				reference to database
				reference to table
				column name
				extra parameters as stored in trigger lookup table
				row1
				row2

				When insert, row1 is the new row, row2 is nil.
				When update, row1 is the new row, row2 is the old row.
				When delete, row1 is the deleted row, row2 is nil.
			*/
			status = TriggerFuncTable()[row["FUNC"]].Execute(db, t, column, strings.Split(strings.TrimSpace(row["PARAM"]), ";"), row1, row2)
			if status != st.OK {
				return status
			}
		}
	}
	return st.OK
}
