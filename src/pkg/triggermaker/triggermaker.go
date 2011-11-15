package triggermaker

import (
	"database"
	"table"
	"st"
	"ra"
	"filter"
)

// Makes a primary key constraint on a column.
func PK(db *database.Database, t *table.Table, name string) (status int) {
	beforeTable, status := db.Get("~before")
	if status != st.OK {
		return
	}
	status = beforeTable.Insert(map[string]string{"TABLE": t.Name, "COLUMN": name, "FUNC": "PK", "OP": "IN"})
	if status != st.OK {
		return
	}
	status = beforeTable.Insert(map[string]string{"TABLE": t.Name, "COLUMN": name, "FUNC": "PK", "OP": "UP"})
	if status != st.OK {
		return
	}
	return beforeTable.Flush()
}

// Makes a foreign key constraint on a column.
func FK(db *database.Database, fkTable *table.Table, fkColumn string, pkTable *table.Table, pkColumn string) (status int) {
	beforeTable, status := db.Get("~before")
	if status != st.OK {
		return
	}
	status = beforeTable.Insert(map[string]string{"TABLE": fkTable.Name, "COLUMN": fkColumn, "FUNC": "FK", "OP": "IN", "PARAM": pkTable.Name + ";" + pkColumn})
	if status != st.OK {
		return
	}
	status = beforeTable.Insert(map[string]string{"TABLE": fkTable.Name, "COLUMN": fkColumn, "FUNC": "FK", "OP": "UP", "PARAM": pkTable.Name + ";" + pkColumn})
	if status != st.OK {
		return
	}
	status = beforeTable.Insert(map[string]string{"TABLE": pkTable.Name, "COLUMN": pkColumn, "FUNC": "UR", "OP": "UP", "PARAM": fkTable.Name + ";" + fkColumn})
	if status != st.OK {
		return
	}
	status = beforeTable.Insert(map[string]string{"TABLE": pkTable.Name, "COLUMN": pkColumn, "FUNC": "DR", "OP": "DE", "PARAM": fkTable.Name + ";" + fkColumn})
	return beforeTable.Flush()
}

// Removes a primary key constraint from a column.
func RemovePK(db *database.Database, t *table.Table, name string) (status int) {
	beforeTable, status := db.Get("~before")
	if status != st.OK {
		return
	}
	// Find trigger rows relevant to the PK constraint.
	query := ra.New()
	query.Load(beforeTable)
	_, status = query.MultipleSelect(ra.Condition{Alias: "TABLE", Filter: filter.Eq{}, Parameter: t.Name},
		ra.Condition{Alias: "COLUMN", Filter: filter.Eq{}, Parameter: name},
		ra.Condition{Alias: "FUNC", Filter: filter.Eq{}, Parameter: "PK"})
	if status != st.OK {
		return
	}
	// Delete each of the rows.
	for _, i := range query.Tables["~before"].RowNumbers {
		status = beforeTable.Delete(i)
		if status != st.OK {
			return
		}
	}
	return beforeTable.Flush()
}
