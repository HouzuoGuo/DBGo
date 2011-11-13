// Relational algebra result.

package ra

import (
	"table"
)

// Table and the selected rows in the table.
type TableResult struct {
	Table *table.Table
	RowNumbers []int
}

// Mapping of table name to column name.
type TableColumn struct {
	TableName, ColumnName string
}

// Relational algebras result.
type Result struct {
	Tables map[string]*TableResult
	Aliases map[string]*TableColumn
}

// Initializes a new Result.
func New() (r *Result) {
	r = new(Result)
	r.Tables = make(map[string]*TableResult)
	r.Aliases = make(map[string]*TableColumn)
	return
}

// Returns a copy of the Result. 
func (r Result) Copy() (*Result) {
	return &r
}

