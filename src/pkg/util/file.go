package util

import (
	"os"
	"st"
	"logg"
)

// Creates a file and writes the content into it.
func CreateAndWrite(filename string, content string) int {
	file, err := os.Create(filename)
	defer file.Close()
	if err != nil {
		logg.Err("util", "CreateAndWrite", err)
		return st.CannotCreateFile
	}
	_, err = file.WriteString(content)
	if err != nil {
		logg.Err("util", "CreateAndWrite", err)
		return st.CannotCreateFile
	}
	return st.OK
}
