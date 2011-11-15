package trigger

import (
	"strings"
	"table"
	"ra"
	"filter"
	"database"
	"st"
)

// Trigger body must implement this interface.
type TriggerFunc interface {
	Execute(db *database.Database, t *table.Table, column string, extraParameters []string, row1, row2 map[string]string) int
}

// Returns a map of trigger function names and trigger body structs.
func TriggerFuncTable() map[string]TriggerFunc {
	return map[string]TriggerFunc{"PK": PK{}, "FK": FK{}}
}

// Executes triggers according to the table operation.
// When insert a new row: row1 is the new row
// When update a row: row1 is the new row, row2 is the old row
// When delete a row: row1 is the old row
func ExecuteTrigger(db *database.Database, t *table.Table, r *ra.Result, operation string, row1, row2 map[string]string) (status int) {
	for column, _ := range row1 {
		raCopy := r.Copy()
		// Filter according to the column name and operation type.
		raCopy.MultipleSelect(ra.Condition{Alias: "COLUMN", Filter: filter.Eq{}, Parameter: column},
			ra.Condition{Alias: "OP", Filter: filter.Eq{}, Parameter: operation})
		// For each trigger.
		for i := 0; i < raCopy.NumberOfRows(); i++ {
			row, status := raCopy.Read(i)
			if status != st.OK {
				return
			}
			// Call the trigger.
			TriggerFuncTable()[row["FUNC"]].Execute(db, t, column, strings.Split(strings.TrimSpace(row["PARAM"]), ";"), row1, row2)
		}
	}
	return
}
