// Error and debugging messages.

package logg

import (
	"fmt"
)

func Err(pkg, function, err interface{}) {
	fmt.Println(fmt.Sprint(pkg) + "." + fmt.Sprint(function) + ":" + fmt.Sprint(err))
}

func Warn(pkg, function, msg interface{}) {
	fmt.Println(fmt.Sprint(pkg) + "." + fmt.Sprint(function) + ":" + fmt.Sprint(msg))
}

func Debug(pkg, function, msg interface{}) {
	fmt.Println(fmt.Sprint(pkg) + "." + fmt.Sprint(function) + ":" + fmt.Sprint(msg))
}
