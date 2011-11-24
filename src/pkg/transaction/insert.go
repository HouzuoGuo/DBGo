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
Insert a row, trigger appropriate triggers and log information for rollback.
*/

package transaction

import (
	"table"
	"st"
	"ra"
	"filter"
	"trigger"
)

type UndoInsert struct {
	Table     *table.Table
	RowNumber int
}

// An insert operation is undone by marking the inserted row deleted.
func (u *UndoInsert) Undo() int {
	return u.Table.Delete(u.RowNumber)
}

func (tr *Transaction) Insert(t *table.Table, row map[string]string) int {
	// Execute "before insert" triggers.
	beforeTable, status := tr.DB.Get("~before")
	if status != st.OK {
		return status
	}
	triggerRA := ra.New()
	_, status = triggerRA.Load(beforeTable)
	if status != st.OK {
		return status
	}
	_, status = triggerRA.Select("TABLE", filter.Eq{}, t.Name)
	if status != st.OK {
		return status
	}
	status = trigger.ExecuteTrigger(tr.DB, t, triggerRA, "IN", row, nil)
	if status != st.OK {
		return status
	}
	// Insert the new row to table.
	numberOfRows, status := t.NumberOfRows()
	if status != st.OK {
		return status
	}
	status = t.Insert(row)
	if status != st.OK {
		return status
	}
	// Execute "after insert" triggers.
	afterTable, status := tr.DB.Get("~after")
	if status != st.OK {
		return status
	}
	triggerRA = ra.New()
	_, status = triggerRA.Load(afterTable)
	if status != st.OK {
		return status
	}
	_, status = triggerRA.Select("TABLE", filter.Eq{}, t.Name)
	if status != st.OK {
		return status
	}
	status = trigger.ExecuteTrigger(tr.DB, t, triggerRA, "IN", row, nil)
	if status != st.OK {
		return status
	}
	// Log the inserted row.
	tr.Log(&UndoInsert{t, numberOfRows})
	return st.OK
}
