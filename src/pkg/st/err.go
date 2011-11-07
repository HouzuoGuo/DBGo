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
	CannotCreateTableDefFile = iota
	CannotCreateTableDataFile = iota
	CannotRenameTableDefFile = iota
	CannotRenameTableDataFile = iota
	ColumnAlreadyExists = iota
	ColumnNameTooLong = iota
)