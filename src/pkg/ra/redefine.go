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

/*
Redefine (renames) an alias in RA result. 
Very useful when joining two tables but they have common column names.
*/

package ra

import (
	"st"
)

// Relational algebra redefine.
func (r *Result) Redefine(oldName, newName string) (*Result, int) {
	_, exists := r.Aliases[oldName]
	if !exists {
		return r, st.AliasNotFound
	}
	_, exists = r.Aliases[newName]
	if exists {
		return r, st.AliasAlreadyExists
	}
	r.Aliases[newName] = r.Aliases[oldName]
	r.Aliases[oldName] = nil, false
	return r, st.OK
}
