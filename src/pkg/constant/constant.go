package constant

const (
	DefFilePerm = 0666
	DataFilePerm = 0666
	TableDirPerm = 0755
	MaxColumnNameLength = 30
	MaxTableNameLength = 30
)

func TableFiles() []string {
	return []string{".data", ".def"}
}

func DatabaseColumns() map[string]int {
	return map[string]int{"~del":1}
}

func TableDirs() []string {
	return []string{".shared"}
}