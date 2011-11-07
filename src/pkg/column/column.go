package column

import (
	"strconv"
	"strings"
	"st"
)

type Column struct {
	Offset, Length int
	Name string
}

func ColumnFromDef(offset int, definition string) (column *Column, status int) {
	lengthName := strings.Split(definition, ":")
	length, err := strconv.Atoi(lengthName[0])
	if err != nil {
		status = st.InvalidColumnDefinition
		return
	}
	column = &Column{Offset:offset, Length:length, Name:lengthName[1]}
	status = st.OK
	return
}

func ColumnToDef(column *Column) (string) {
	return column.Name + ":" + string(column.Length)
}