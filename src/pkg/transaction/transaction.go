// Transaction management.
package transaction

import (
	"table"
	"database"
	"time"
	"strconv"
)

// An undoable operation such as insert, update and delete.
type Undoable interface {
	Undo() int
}

type Transaction struct {
	DB     *database.Database
	Done   []Undoable
	ID     string
	Locked []*table.Table
}

// Returns a new and ready Transaction.
func New(db *database.Database) *Transaction {
	return &Transaction{db, make([]Undoable, 0), strconv.Itoa64(time.Nanoseconds()), make([]*table.Table, 0)}
}

// Logs a table operation.
func (tr *Transaction) Log(undoable Undoable) {
	tr.Done = append(tr.Done[:], undoable)
}
