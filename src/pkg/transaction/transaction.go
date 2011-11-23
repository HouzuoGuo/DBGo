// Transaction management.
package transaction

import (
	"table"
	"database"
	"time"
	"strconv"
	"st"
)

// An undoable operation such as insert, update and delete.
type Undoable interface {
	Undo() int
}

type Transaction struct {
	DB     *database.Database
	Done   []Undoable // completed table operations (insert, update, delete)
	ID     string     // transaction ID as string
	id     int64      // identical to ID, but in int type
	Locked []*table.Table
}

// Returns a new and ready Transaction.
func New(db *database.Database) *Transaction {
	theID := time.Nanoseconds()
	return &Transaction{db, make([]Undoable, 0), strconv.Itoa64(theID), theID, make([]*table.Table, 0)}
}

// Logs a table operation.
func (tr *Transaction) Log(undoable Undoable) {
	tr.Done = append(tr.Done[:], undoable)
}

// Commits the transaction and release locked tables.
func (tr *Transaction) Commit() int {
	status := int(st.OK)
	for _, table := range tr.Locked {
		status = table.Flush()
		if status != st.OK {
			return status
		}
		status = tr.unlock(table)
		if status != st.OK {
			return status
		}
	}
	tr.Locked = make([]*table.Table, 0)
	tr.Done = make([]Undoable, 0)
	return status
}

// Rolls back transaction and release locked tables.
func (tr *Transaction) Rollback() int {
	status := int(st.OK)
	for i := len(tr.Done) - 1; i >= 0; i-- {
		status = tr.Done[i].Undo()
		if status != st.OK {
			break
		}
	}
	// Error happening during undo may be more serious than failure of releasing locks.
	if status == st.OK {
		status = tr.Commit()
	} else {
		tr.Commit()
	}
	return status
}
