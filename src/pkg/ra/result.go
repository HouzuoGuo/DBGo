// Relational algebra results.

package ra

import (
	"strings"
	"constant"
	"table"
	"logg"
	"fmt"
	"st"
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

// Relational algebras result. For convenience, the thingy is called RA result.
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

// Load all rows of a table into RA result.
func (r *Result) Load(t *table.Table) (self *Result, status int) {
	_, exists := r.Tables[t.Name]
	if exists {
		return r, st.TableAlreadyExists
	}
	// rowNumbers = list(range(t.NumberOfRows()))
	rowNumbers := make([]int, 0)
	numberOfRows, status := t.NumberOfRows()
	if status != st.OK {
		return r, status
	}
	for i := 0; i < numberOfRows; i++ {
		rowNumbers = append(rowNumbers[:], i)
	}
	r.Tables[t.Name] = &TableResult{t, rowNumbers}
	// Load columns of the table.
	for columnName, _ := range t.Columns {
		if !strings.HasPrefix(columnName, constant.ThePrefix) {
			r.Aliases[columnName] = &TableColumn{t.Name, columnName}
		}
	}
	return r, st.OK
}

// For debugging purpose, prints the RA result.
func (r *Result) Report() {
	var content string
	for name, t := range r.Tables {
		content += "Table: " + name + "\t" + fmt.Sprint(t.RowNumbers) + "\n";
	}
	for alias, c := range r.Aliases {
		content += "Alias " + alias + "\tis " + c.TableName + "." + c.ColumnName + "\n";
	}
	logg.Debug("ra", "Report", content)
}