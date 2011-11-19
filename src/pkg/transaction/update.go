// UPDATE ... SET ...
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
