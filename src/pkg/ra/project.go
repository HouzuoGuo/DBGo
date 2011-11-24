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

/* Keep only the specified columns in RA result.*/

package ra

import (
	"st"
)

// Relational algebra project.
func (r *Result) Project(aliases ...string) (*Result, int) {
	for presentAlias, _ := range r.Aliases {
		found := false
		tableName := r.Aliases[presentAlias].TableName
		for _, alias := range aliases {
			if alias == presentAlias {
				found = true
				break
			}
		}
		// Remove the alias from RA result if we do not wish to keep it.
		if !found {
			r.Aliases[presentAlias] = nil, false
			// Count how many tables are still using the table of the removed alias.
			count := 0
			for _, column := range r.Aliases {
				if column.TableName == tableName {
					count++
				}
			}
			// If the table is no longer used, remove it from RA result.
			if count == 0 {
				r.Tables[tableName] = nil, false
			}
		}
	}
	return r, st.OK
}
