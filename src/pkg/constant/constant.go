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

/* Constants which will affect DBGo runtime databases. */

package constant

const (
	DefFilePerm               = 0666 // permission for opening .def file of table 
	DataFilePerm              = 0666 // permission for opening .data file of table
	TableDirPerm              = 0755 // permission for creating table directory
	InitFilePerm              = 0666 // permission for opening .init file of database
	MaxColumnNameLength       = 30
	MaxTableNameLength        = 30
	ThePrefix                 = "~" // do not use this prefix to name a database thingy
	MaxTriggerFuncNameLength  = 50
	MaxTriggerParameterLength = 200
	TriggerOperationLength    = 4
	LockTimeout               = 60000000000 // (60 seconds) timeout of table locks (shared & exclusive) in nanoseconds
	ExclusiveLockFilePerm     = 0666        // permission for opening .exclusive file of table lock
)

// Returns the extension names which table files have.
func TableFiles() []string {
	return []string{".data", ".def"}
}

// Returns the column names and lengths which a new table have. 
func DatabaseColumns() map[string]int {
	return map[string]int{ThePrefix + "del": 1}
}

// Returns the directory suffixes which table directories have. 
func TableDirs() []string {
	return []string{".shared"}
}

// Returns the column names and lengths of a database trigger lookup table.
func TriggerLookupTable() map[string]int {
	return map[string]int{"TABLE": MaxTableNameLength, "COLUMN": MaxColumnNameLength,
		"FUNC": MaxTriggerFuncNameLength, "OP": TriggerOperationLength,
		"PARAM": MaxTriggerParameterLength}
}
