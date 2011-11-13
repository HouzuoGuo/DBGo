package ra

import (
	"filter"
	"st"
)

func (r *Result) Select ( alias string, filter *filter.Filter, parameter string) (self *Result, status int) {
	tableName := r.Aliases[alias].TableName
	columnName := r.Aliases[alias].ColumnName
	table := r.Tables[tableName].Table
	rowNumbers := r.Tables[tableName].RowNumbers
	kept := make([]int, 0)
	for i := 0; i < len(rowNumbers); i++ {
		row, status := table.Read(rowNumbers[i])
		if status != st.OK {
			return
		}
		if row["~del"] != "y" && filter.Cmp(row[columnName], parameter) {
			kept = append(kept[:], i) 
		}
	}
	return self, status
}