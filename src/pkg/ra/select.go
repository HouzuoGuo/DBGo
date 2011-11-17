package ra

import (
	"filter"
	"st"
)

// Relational algebra select.
func (r *Result) Select(alias string, filter filter.Filter, parameter interface{}) (*Result,  int) {
	tableName := r.Aliases[alias].TableName
	columnName := r.Aliases[alias].ColumnName
	table := r.Tables[tableName].Table
	rowNumbers := r.Tables[tableName].RowNumbers
	kept := make([]int, 0)
	// Iterate through the rows of the table in the RA result.
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
	// Keep only the kept rows, for all tables in the RA result.
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
	Alias     string
	Filter    filter.Filter
	Parameter interface{}
}

// Same as relational algebra select but accepts multiple conditions.
func (r *Result) MultipleSelect(conditions ...Condition) (*Result, int) {
	for _, condition := range conditions {
		_, status := r.Select(condition.Alias, condition.Filter, condition.Parameter)
		if status != st.OK {
			return r, status
		}
	}
	return r, st.OK
}
