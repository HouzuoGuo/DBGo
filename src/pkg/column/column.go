package column

import (
	"strconv"
	"strings"
	"st"
	"logg"
)

type Column struct {
	Offset, Length int
	Name string
}

func ColumnFromDef(offset int, definition string) (column *Column, status int) {
	lengthName := strings.Split(definition, ":")
	length, err := strconv.Atoi(lengthName[1])
	if err != nil {
		logg.Err("Column", "ColumnFromDef", "Definition malformed: " + definition)
		status = st.InvalidColumnDefinition
		return
	}
	column = &Column{Offset:offset, Length:length, Name:lengthName[0]}
	status = st.OK
	return
}

func ColumnToDef(column *Column) (string) {	
	return column.Name + ":" + strconv.Itoa(column.Length) + "\n"
}