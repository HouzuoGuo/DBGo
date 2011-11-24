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

/* Filters are used by relational algebras to filter rows in Select operation. */

package filter

import (
	"strconv"
	"fmt"
)

type Filter interface {
	Cmp(v1, v2 interface{}) bool
}

type Eq struct {

}

// Tests if two strings are equal.
func (f Eq) Cmp(v1, v2 interface{}) bool {
	return fmt.Sprint(v1) == fmt.Sprint(v2)
}

type Lt struct {

}
// Tests if value 1 is less than value2. The values are converted to double before comparison. 
// Always returns false if number format is unexpected.
func (f Lt) Cmp(v1, v2 interface{}) bool {
	d1, err := strconv.Atof64(fmt.Sprint(v1))
	if err != nil {
		return false
	}
	d2, err := strconv.Atof64(fmt.Sprint(v2))
	if err != nil {
		return false
	}
	return d1 < d2
}

type Gt struct {

}
// Tests if value 1 is greater than value2. The values are converted to double before comparison. 
// Always returns false if number format is unexpected.
func (f Gt) Cmp(v1, v2 interface{}) bool {
	d1, err := strconv.Atof64(fmt.Sprint(v1))
	if err != nil {
		return false
	}
	d2, err := strconv.Atof64(fmt.Sprint(v2))
	if err != nil {
		return false
	}
	return d1 > d2
}
