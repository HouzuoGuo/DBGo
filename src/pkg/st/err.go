package st

const (
	CannotOpenDatabaseDirectory = 1
	CannotReadDatabaseDirectory = 2
	CannotOpenTableFiles = 3
	CannotOpenTableDefFile = 4
	CannotOpenTableDataFile = 5
	InvalidColumnDefinition = 6
	CannotStatTableDefFile = 7
	CannotStatTableDataFile = 8
	CannotSeekTableDataFile = 9
	CannotSeekTableDefFile = 10
	CannotReadTableDataFile = 11
	CannotWriteTableDataFile = 12
	CannotWriteTableDefFile = 13
	CannotFlushTableDefFile = 14
	CannotFlushTableDataFile = 15
	TableDoesNotHaveDelColumn = 16
	TableNameTooLong = 17
	TableAlreadyExists = 18
	CannotCreateTableFile = 19
	CannotCreateTableDir = 20
	CannotRenameTableFile = 21
	CannotRenameTableDir = 22
	CannotRemoveTableFile = 23
	CannotRemoveTableDir = 24
	ColumnAlreadyExists = 25
	ColumnNameTooLong = 26
	TableNotFound = 27
)