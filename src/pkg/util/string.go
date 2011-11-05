package util

import (
	"strings"
)

// Returns a string which is the original string trimmed to the desired length.
// Trailing spaces are added if the string's length is too short.
// Otherwise, the string is truncated from right to the desired length.
func TrimLength(str string, length int) (trimmed string) {
	lengthDiff := length - len(str)
	if lengthDiff > 0 {
		trimmed = str + strings.Repeat(" ", lengthDiff)
	} else {
		trimmed = str[:length]
	}
	return trimmed
}

// Returns file name (without extension) and extension of a file name.
func FilenameParts(filename string) (name, extension string) {
	dotIndex := strings.LastIndex(filename, ".")
	if dotIndex == -1 || dotIndex == len(filename) - 1 {
		name = filename
		extension = ""
	} else {
		name = filename[:dotIndex]
		extension = filename[dotIndex + 1:]
	}
	return
}