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
Update a row, trigger appropriate triggers and log information for rollback.
*/

package transaction

import (
	"table"
	"st"
	"ra"
	"filter"
	"trigger"
)

type UndoUpdate struct {
	Table     *table.Table
	RowNumber int
	Original  map[string]string
}

// An insert operation is undone by marking the inserted row deleted.
func (u *UndoUpdate) Undo() int {
	return u.Table.Update(u.RowNumber, u.Original)
}

func (tr *Transaction) Update(t *table.Table, rowNumber int, row map[string]string) int {
	// Execute "before update" triggers.
	beforeTable, status := tr.DB.Get("~before")
	if status != st.OK {
		return status
	}
	original, status := t.Read(rowNumber)
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
	status = trigger.ExecuteTrigger(tr.DB, t, triggerRA, "UP", row, original)
	if status != st.OK {
		return status
	}
	// Update the row.
	status = t.Update(rowNumber, row)
	if status != st.OK {
		return status
	}
	// Execute "after update" triggers.
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
	status = trigger.ExecuteTrigger(tr.DB, t, triggerRA, "UP", row, original)
	if status != st.OK {
		return status
	}
	// Log the updated row.
	tr.Log(&UndoUpdate{t, rowNumber, original})
	return st.OK
}
