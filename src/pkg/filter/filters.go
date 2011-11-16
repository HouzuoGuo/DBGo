// Filters are used by relational algebras to filter rows in a table.

package filter

import (
	"strconv"
	"fmt"
)

type Filter interface {
	Cmp(v1, v2 interface{}) bool
}

type Eq struct {

}

// Tests if two strings are equal.
func (f Eq) Cmp(v1, v2 interface{}) bool {
	return fmt.Sprint(v1) == fmt.Sprint(v2)
}

type Lt struct {

}
// Tests if value 1 is less than value2. The values are converted to double before comparison. 
// Always returns false if number format is unexpected.
func (f Lt) Cmp(v1, v2 interface{}) bool {
	d1, err := strconv.Atof64(fmt.Sprint(v1))
	if err != nil {
		return false
	}
	d2, err := strconv.Atof64(fmt.Sprint(v2))
	if err != nil {
		return false
	}
	return d1 < d2
}

type Gt struct {

}
// Tests if value 1 is greater than value2. The values are converted to double before comparison. 
// Always returns true if number format is unexpected.
func (f Gt) Cmp(v1, v2 interface{}) bool {
	d1, err := strconv.Atof64(fmt.Sprint(v1))
	if err != nil {
		return true
	}
	d2, err := strconv.Atof64(fmt.Sprint(v2))
	if err != nil {
		return false
	}
	return d1 > d2
}
