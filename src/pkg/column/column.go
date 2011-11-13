// Conversions between text definition of a column and a Column.

package column

import (
	"strconv"
	"strings"
	"st"
	"logg"
)

type Column struct {
	Offset int // offset of the column in row
	Length int // max length of the column's value
	Name   string
}

// Constructs a Column from a column's text definition.
func ColumnFromDef(offset int, definition string) (column *Column, status int) {
	// Extract length and name from the definition which should be "name:length"
	lengthName := strings.Split(definition, ":")
	length, err := strconv.Atoi(lengthName[1])
	if err != nil {
		logg.Err("Column", "ColumnFromDef", "Definition malformed: "+definition)
		return nil, st.InvalidColumnDefinition
	}
	column = &Column{Offset: offset, Length: length, Name: lengthName[0]}
	return
}

// Constructs a text definition of a column.
func ColumnToDef(column *Column) string {
	return column.Name + ":" + strconv.Itoa(column.Length) + "\n"
}
