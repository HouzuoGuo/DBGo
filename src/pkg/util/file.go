package util

import (
	"os"
	"strings"
	"st"
	"logg"
)

// Creates a file and writes the content into it.
func CreateAndWrite(filename, content string) int {
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

// Removes a line's occurances from a file.
func RemoveLine(filename, line string) int {
	// Open and read the file.
	file, err := os.Open(filename)
	if err != nil {
		logg.Err("util", "RemoveLine", err)
		return st.CannotReadFile
	}
	fi, err := file.Stat()
	if err != nil {
		logg.Err("util", "RemoveLine", err)
		return st.CannotReadFile
	}
	buffer := make([]byte, fi.Size)
	_, err = file.Read(buffer)
	if err != nil {
		logg.Err("util", "RemoveLine", err)
		return st.CannotReadFile
	}
	file.Close()
	// Re-open the file and overwrite it.
	file, err = os.OpenFile(filename, os.O_WRONLY+os.O_TRUNC, 0666)
	defer file.Close()
	if err != nil {
		logg.Err("util", "RemoveLine", err)
		return st.CannotReadFile
	}
	lines := strings.Split(string(buffer), "\n")
	for _, content := range lines {
		if strings.TrimSpace(content) != strings.TrimSpace(line) {
			_, err = file.WriteString(content + "\n")
			if err != nil {
				logg.Err("util", "RemoveLine", err)
				return st.CannotWriteFile
			}
		}
	}
	return st.OK
}
