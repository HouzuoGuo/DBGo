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

/* Deals with database error, warning and debug messages. */

package logg

import (
	"fmt"
)

func Err(pkg, function, err interface{}) {
	fmt.Println(fmt.Sprint(pkg) + "." + fmt.Sprint(function) + ":" + fmt.Sprint(err))
}

func Warn(pkg, function, msg interface{}) {
	fmt.Println(fmt.Sprint(pkg) + "." + fmt.Sprint(function) + ":" + fmt.Sprint(msg))
}

func Debug(pkg, function, msg interface{}) {
	fmt.Println(fmt.Sprint(pkg) + "." + fmt.Sprint(function) + ":" + fmt.Sprint(msg))
}
