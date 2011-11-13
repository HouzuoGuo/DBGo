// Filters are used by relational algebras to filter rows in a table.

package filter

type Filter interface {
	Cmp(v1, v2 string) bool
}
