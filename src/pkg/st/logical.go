// Logical errors.
// Data is ensured to be safe and consistent when these codes are raised.

package st

const (
	DuplicatedPKValue     = 301
	InvalidFKValue        = 302
	DeleteRestricted      = 303
	UpdateRestricted      = 304
	CannotLockInExclusive = 305
	CannotLockInShared    = 306
	DuplicatedAlias       = 307
)
