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
Error codes.
Some of these codes may indicate data loss or issues with file system.
These error codes will immediately halt the function when they happen.
*/

package st

const (
	CannotOpenDatabaseDirectory  = 100
	CannotReadDatabaseDirectory  = 101
	CannotOpenTableFiles         = 102
	CannotOpenTableDefFile       = 103
	CannotOpenTableDataFile      = 104
	InvalidColumnDefinition      = 105
	CannotStatTableDefFile       = 106
	CannotStatTableDataFile      = 107
	CannotSeekTableDataFile      = 108
	CannotSeekTableDefFile       = 109
	CannotReadTableDataFile      = 110
	CannotWriteTableDataFile     = 111
	CannotWriteTableDefFile      = 112
	CannotFlushTableDefFile      = 113
	CannotFlushTableDataFile     = 114
	TableDoesNotHaveDelColumn    = 115
	TableNameTooLong             = 116
	TableAlreadyExists           = 117
	CannotCreateTableFile        = 118
	CannotCreateTableDir         = 119
	CannotRenameTableFile        = 120
	CannotRenameTableDir         = 121
	CannotRemoveTableFile        = 122
	CannotRemoveTableDir         = 123
	ColumnAlreadyExists          = 124
	ColumnNameTooLong            = 125
	TableNotFound                = 126
	InvalidColumnLength          = 127
	AliasNotFound                = 128
	AliasAlreadyExists           = 129
	CannotCreateInitFile         = 130
	CannotReadSharedLocksDir     = 131
	CannotReadExclusiveLocksFile = 132
	CannotUnlockSharedLock       = 133
	CannotUnlockExclusiveLock    = 134
	CannotCreateFile             = 135
	CannotReadFile               = 136
	CannotWriteFile              = 137
	CannotRemoveSpecialColumn    = 138
)
