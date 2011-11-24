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
Database logic errors.
Data is ensured to be safe and consistent when these codes are raised.
*/

package st

const (
	DuplicatedPKValue     = 301
	InvalidFKValue        = 302
	DeleteRestricted      = 303
	UpdateRestricted      = 304
	CannotLockInExclusive = 305
	CannotLockInShared    = 306
	DuplicatedAlias       = 307
)
