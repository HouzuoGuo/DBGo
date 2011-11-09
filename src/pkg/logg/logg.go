package logg

import (
	"fmt"
)

func Err(pkg, function, err interface{}) {
	fmt.Println("Error:", pkg, ".", function, err)
}

func Debug(pkg, function, msg interface{}) {
	fmt.Println("Debug:", pkg, ".", function, msg)
}