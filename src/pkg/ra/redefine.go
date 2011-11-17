package ra

import (
	"st"
)

// Relational algebra redefine.
func (r *Result) Redefine(oldName, newName string) (*Result, int) {
	_, exists := r.Aliases[oldName]
	if !exists {
		return r, st.AliasNotFound
	}
	_, exists = r.Aliases[newName]
	if exists {
		return r, st.AliasAlreadyExists
	}
	r.Aliases[newName] = r.Aliases[oldName]
	r.Aliases[oldName] = nil, false
	return r, st.OK
}
