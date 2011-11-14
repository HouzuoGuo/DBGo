package ra

import (
	"filter"
	"st"
)

// Relational algebra select.
func (r *Result) Select ( alias string, filter filter.Filter, parameter string) (self *Result, status int) {
	tableName := r.Aliases[alias].TableName
	columnName := r.Aliases[alias].ColumnName
	table := r.Tables[tableName].Table
	rowNumbers := r.Tables[tableName].RowNumbers
	kept := make([]int, 0)
	// Iterate through the rows of the table in the RA result.
	for i := 0; i < len(rowNumbers); i++ {
		row, status := table.Read(rowNumbers[i])
		if status != st.OK {
			return
		}
		// Keep the row if it passes the filter and is not a deleted row.
		if row["~del"] != "y" && filter.Cmp(row[columnName], parameter) {
			kept = append(kept[:], i) 
		}
	}
	// Keep only the kept rows, for all tables in the RA result.
	for _, table := range r.Tables {
		newRowNumbers := make([]int, len(kept))
		for i, keep := range kept {
			newRowNumbers[i] = table.RowNumbers[keep];
		}
		table.RowNumbers = newRowNumbers;
	}
	return self, status
}