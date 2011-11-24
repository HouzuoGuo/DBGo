/*
<DBGo - A flat-file relational database engine implementation in Go programming language>
Copyright (C) <2011>  <Houzuo (Howard) Guo>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

/* Making/removing PK/FK constraints and triggers. */

package constraint

import (
	"database"
	"table"
	"st"
	"ra"
	"filter"
)

// Makes a primary key constraint on a column.
func PK(db *database.Database, t *table.Table, name string) int {
	beforeTable, status := db.Get("~before")
	if status != st.OK {
		return status
	}
	// On PK table and PK column, triggers PK function before insert.
	status = beforeTable.Insert(map[string]string{"TABLE": t.Name, "COLUMN": name, "FUNC": "PK", "OP": "IN"})
	if status != st.OK {
		return status
	}
	// On PK table and PK column, triggers PK function before update.
	status = beforeTable.Insert(map[string]string{"TABLE": t.Name, "COLUMN": name, "FUNC": "PK", "OP": "UP"})
	if status != st.OK {
		return status
	}
	return beforeTable.Flush()
}

// Makes a foreign key constraint on a column, together with delete/update restricted triggers.
func FK(db *database.Database, fkTable *table.Table, fkColumn string, pkTable *table.Table, pkColumn string) int {
	/* 
		In fact, the "pkColumn" in "pkTable" does not have to have PK constraint.
		FK constraint will still function properly in that case. 
	*/
	beforeTable, status := db.Get("~before")
	if status != st.OK {
		return status
	}
	// On FK table and FK column, triggers FK function before insert.
	status = beforeTable.Insert(map[string]string{"TABLE": fkTable.Name, "COLUMN": fkColumn, "FUNC": "FK", "OP": "IN", "PARAM": pkTable.Name + ";" + pkColumn})
	if status != st.OK {
		return status
	}
	// On FK table and FK column, triggers FK function before update.
	status = beforeTable.Insert(map[string]string{"TABLE": fkTable.Name, "COLUMN": fkColumn, "FUNC": "FK", "OP": "UP", "PARAM": pkTable.Name + ";" + pkColumn})
	if status != st.OK {
		return status
	}
	// On PK table and PK column, triggers UR (update restricted) function before update.
	status = beforeTable.Insert(map[string]string{"TABLE": pkTable.Name, "COLUMN": pkColumn, "FUNC": "UR", "OP": "UP", "PARAM": fkTable.Name + ";" + fkColumn})
	if status != st.OK {
		return status
	}
	// On PK table and PK column, triggers DR (delete restricted) function before delete.
	status = beforeTable.Insert(map[string]string{"TABLE": pkTable.Name, "COLUMN": pkColumn, "FUNC": "DR", "OP": "DE", "PARAM": fkTable.Name + ";" + fkColumn})
	return beforeTable.Flush()
}

// Deletes rows in a table of RA result according to some select conditions.
// The RA result is made a copy before using select conditions.
func findAndDelete(t *table.Table, query *ra.Result, conditions ...ra.Condition) int {
	_, status := query.Copy().MultipleSelect(conditions...)
	if status != st.OK {
		return status
	}
	for _, i := range query.Tables[t.Name].RowNumbers {
		status = t.Delete(i)
		if status != st.OK {
			return status
		}
	}
	return t.Flush()
}

// Removes primary key constraint from a column.
func RemovePK(db *database.Database, t *table.Table, name string) int {
	beforeTable, status := db.Get("~before")
	if status != st.OK {
		return status
	}
	query := ra.New()
	query.Load(beforeTable)
	return findAndDelete(beforeTable, query,
		ra.Condition{Alias: "TABLE", Filter: filter.Eq{}, Parameter: t.Name},
		ra.Condition{Alias: "COLUMN", Filter: filter.Eq{}, Parameter: name},
		ra.Condition{Alias: "FUNC", Filter: filter.Eq{}, Parameter: "PK"})
}

// Removes foreign key constraint from a column, together with delete/update restricted triggers.
func RemoveFK(db *database.Database, fkTable *table.Table, fkColumn string, pkTable *table.Table, pkColumn string) int {
	beforeTable, status := db.Get("~before")
	if status != st.OK {
		return status
	}
	query := ra.New()
	query.Load(beforeTable)
	// Remove FK constraint on FK column.
	status = findAndDelete(beforeTable, query,
		ra.Condition{Alias: "TABLE", Filter: filter.Eq{}, Parameter: fkTable.Name},
		ra.Condition{Alias: "COLUMN", Filter: filter.Eq{}, Parameter: fkColumn},
		ra.Condition{Alias: "FUNC", Filter: filter.Eq{}, Parameter: "FK"},
		ra.Condition{Alias: "PARAM", Filter: filter.Eq{}, Parameter: pkTable.Name + ";" + pkColumn})
	if status != st.OK {
		return status
	}
	// Remove delete restricted trigger.
	status = findAndDelete(beforeTable, query,
		ra.Condition{Alias: "TABLE", Filter: filter.Eq{}, Parameter: pkTable.Name},
		ra.Condition{Alias: "COLUMN", Filter: filter.Eq{}, Parameter: pkColumn},
		ra.Condition{Alias: "FUNC", Filter: filter.Eq{}, Parameter: "DR"},
		ra.Condition{Alias: "PARAM", Filter: filter.Eq{}, Parameter: fkTable.Name + ";" + fkColumn})
	if status != st.OK {
		return status
	}
	// Remove update restricted trigger.
	return findAndDelete(beforeTable, query,
		ra.Condition{Alias: "TABLE", Filter: filter.Eq{}, Parameter: pkTable.Name},
		ra.Condition{Alias: "COLUMN", Filter: filter.Eq{}, Parameter: pkColumn},
		ra.Condition{Alias: "FUNC", Filter: filter.Eq{}, Parameter: "UR"},
		ra.Condition{Alias: "PARAM", Filter: filter.Eq{}, Parameter: fkTable.Name + ";" + fkColumn})
}
