// DELETE row number N
package transaction

import (
	"table"
	"st"
	"ra"
	"filter"
	"trigger"
)

type UndoDelete struct {
	Table     *table.Table
	RowNumber int
}

// An insert operation is undone by marking the inserted row deleted.
func (u *UndoDelete) Undo() int {
	return u.Table.Update(u.RowNumber, map[string]string{"~del": ""})
}

func (tr *Transaction) Delete(t *table.Table, rowNumber int) int {
	// Execute "before delete" triggers.
	beforeTable, status := tr.DB.Get("~before")
	if status != st.OK {
		return status
	}
	row, status := t.Read(rowNumber)
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
	status = trigger.ExecuteTrigger(tr.DB, t, triggerRA, "DE", row, nil)
	if status != st.OK {
		return status
	}
	// Update the row.
	status = t.Delete(rowNumber)
	if status != st.OK {
		return status
	}
	// Execute "after delete" triggers.
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
	status = trigger.ExecuteTrigger(tr.DB, t, triggerRA, "DE", row, nil)
	if status != st.OK {
		return status
	}
	// Log the deleted row.
	tr.Log(&UndoDelete{t, rowNumber})
	return st.OK
}
