package ra

import (
	"st"
	"logg"
)

// Relational algebra project.
func (r *Result) Project(aliases ...string) (self *Result, status int) {
	for presentAlias, _ := range r.Aliases {
		found := false
		tableName := r.Aliases[presentAlias].TableName
		logg.Debug("Table name is", tableName, "")
		for _, alias := range aliases {
			if alias == presentAlias {
				found = true
				break
			}
		}
		// Remove the alias from RA result if we do not wish to keep it.
		if !found {
			r.Aliases[presentAlias] = nil, false
			// Count how many tables are still using the table of the removed alias.
			count := 0
			for _, column := range r.Aliases {
				if column.TableName == tableName {
					count++
				}
			}
			// If the table is no longer used, remove it from RA result.
			if count == 0 {
				r.Tables[tableName] = nil, false
			}
		}
	}
	return r, st.OK
}