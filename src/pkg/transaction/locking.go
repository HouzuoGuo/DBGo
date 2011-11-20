// Locking mechanisms.

package transaction

import (
	"os"
	"strconv"
	"constant"
	"table"
	"st"
	"fmt"
)

type Locks struct {
	Shared    []int64
	Exclusive int64
}

// Returns existing shared and exclusive locks of a table.
func (tr *Transaction) LocksOf(t *table.Table) (*Locks, int) {
	// Read files in .shared directory.
	sharedLocksPath := t.Path + t.Name + ".shared"
	fmt.Println(sharedLocksPath)
	sharedLocksDir, err := os.Open(sharedLocksPath)
	if err != nil {
		return nil, st.CannotReadSharedLocksDir
	}
	defer sharedLocksDir.Close()
	fi, err := sharedLocksDir.Readdir(0)
	if err != nil {
		return nil, st.CannotReadSharedLocksDir
	}
	locks := new(Locks)
	locks.Shared = make([]int64, 0)
	for _, fileInfo := range fi {
		// File name represents a transaction ID (also a timestamp).
		theID, err := strconv.Atoi64(fileInfo.Name)
		if err != nil || theID > constant.LockTimeout {
			// Remove expired shared lock.
			err = os.Remove(sharedLocksPath + "/" + fileInfo.Name)
			if err != nil {
				return nil, st.CannotUnlockSharedLock
			}
		} else {
			locks.Shared = append(locks.Shared[:], theID)
		}
	}
	// Read the content of exclusive lock.
	exclusiveLockPath := t.Path + t.Name + ".exclusive"
	fmt.Println(exclusiveLockPath)
	exclusiveFile, err := os.Open(exclusiveLockPath)
	if err != nil {
		return locks, st.OK
	}
	fi2, err := exclusiveFile.Stat()
	if err != nil {
		return nil, st.CannotReadExclusiveLocksFile
	}
	// The file content is a transaction ID
	buffer := make([]byte, fi2.Size)
	_, err = exclusiveFile.Read(buffer)
	if err != nil {
		return nil, st.CannotReadExclusiveLocksFile
	}
	theID, err := strconv.Atoi64(string(buffer))
	if err != nil || theID > constant.LockTimeout {
		// Remove expired exclusive lock.
		err = os.Remove(exclusiveLockPath)
		if err != nil {
			return nil, st.CannotUnlockExclusiveLock
		}
	} else {
		locks.Exclusive = theID
	}
	return locks, st.OK
}
