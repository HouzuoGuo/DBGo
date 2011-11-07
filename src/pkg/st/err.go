package st

const (
	CannotOpenDatabaseDirectory = iota
	CannotReadDatabaseDirectory = iota
	CannotOpenTableFiles = iota
	CannotOpenTableDefFile = iota
	CannotOpenTableDataFile = iota
	InvalidColumnDefinition = iota
	CannotStatTableDefFile = iota
	CannotStatTableDataFile = iota
	CannotSeekTableDataFile = iota
	CannotSeekTableDefFile = iota
	CannotReadTableDataFile = iota
	CannotWriteTableDataFile = iota
	CannotWriteTableDefFile = iota
	CannotFlushTableDefFile = iota
	CannotFlushTableDataFile = iota
	TableDoesNotHaveDelColumn = iota
	TableNameTooLong = iota
	TableAlreadyExists = iota
	CannotCreateTableFile = iota
	CannotCreateTableDir = iota
	CannotRenameTableFile = iota
	CannotRenameTableDir = iota
	CannotRemoveTableFile = iota
	CannotRemoveTableDir = iota
	ColumnAlreadyExists = iota
	ColumnNameTooLong = iota
	TableNotFound = iota
)