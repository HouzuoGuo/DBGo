/*
<DBGo - A flat-file relational database engine implementation in Go programming language>
Copyright (C) <2011>  <Houzuo (Howard) Guo>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

/* Exclusive, shared table locks, unlocking. */

package transaction

import (
	"os"
	"strconv"
	"time"
	"constant"
	"table"
	"st"
	"util"
	"logg"
)

type Locks struct {
	Shared    []int64
	Exclusive int64
}

// Returns existing shared and exclusive locks of a table.
func LocksOf(t *table.Table) (*Locks, int) {
	// Read files in .shared directory.
	sharedLocksPath := t.Path + t.Name + ".shared"
	sharedLocksDir, err := os.Open(sharedLocksPath)
	if err != nil {
		return nil, st.CannotReadSharedLocksDir
	}
	defer sharedLocksDir.Close()
	fi, err := sharedLocksDir.Readdir(0)
	if err != nil {
		logg.Err("transaction", "LocksOf", err)
		return nil, st.CannotReadSharedLocksDir
	}
	locks := new(Locks)
	locks.Shared = make([]int64, 0)
	for _, fileInfo := range fi {
		// File name represents a transaction ID (also a timestamp).
		theID, err := strconv.Atoi64(fileInfo.Name)
		if err != nil || theID > time.Nanoseconds()+constant.LockTimeout {
			// Remove expired shared lock.
			err = os.Remove(sharedLocksPath + "/" + fileInfo.Name)
			logg.Warn("transaction", "LocksOf", "Expired shared lock ID "+
				fileInfo.Name+" file "+sharedLocksPath+"/"+fileInfo.Name+" is removed")
			if err != nil {
				logg.Err("transaction", "LocksOf", err)
				return nil, st.CannotUnlockSharedLock
			}
		} else {
			locks.Shared = append(locks.Shared[:], theID)
		}
	}
	// Read the content of exclusive lock.
	exclusiveLockPath := t.Path + t.Name + ".exclusive"
	exclusiveFile, err := os.Open(exclusiveLockPath)
	if err != nil {
		return locks, st.OK
	}
	fi2, err := exclusiveFile.Stat()
	if err != nil {
		logg.Err("transaction", "LocksOf", err)
		return nil, st.CannotReadExclusiveLocksFile
	}
	// The file content is a transaction ID
	buffer := make([]byte, fi2.Size)
	_, err = exclusiveFile.Read(buffer)
	if err != nil {
		logg.Err("transaction", "LocksOf", err)
		return nil, st.CannotReadExclusiveLocksFile
	}
	theID, err := strconv.Atoi64(string(buffer))
	if err != nil || theID > time.Nanoseconds()+constant.LockTimeout {
		// Remove expired exclusive lock.
		err = os.Remove(exclusiveLockPath)
		logg.Debug("transaction", "LocksOf", err)
		logg.Warn("transaction", "LocksOf", "Expired exclusive lock ID "+
			string(buffer)+" file "+exclusiveLockPath+" is removed")
		if err != nil {
			logg.Err("transaction", "LocksOf", err)
			return nil, st.CannotUnlockExclusiveLock
		}
	} else {
		locks.Exclusive = theID
	}
	return locks, st.OK
}

// Locks a table in exclusive mode.
func (tr *Transaction) ELock(t *table.Table) int {
	existingLocks, status := LocksOf(t)
	if status != st.OK {
		return status
	}
	// Do not lock if other transaction(s) have shared or exclusively locked the table.
	if (existingLocks.Exclusive != 0 && existingLocks.Exclusive != tr.id) ||
		(len(existingLocks.Shared) == 1 && existingLocks.Shared[0] != tr.id) ||
		(len(existingLocks.Shared) > 1) {
		return st.CannotLockInExclusive
	}
	// Release previously acquired shared lock, if any. 
	if len(existingLocks.Shared) == 1 && existingLocks.Shared[0] == tr.id {
		status = tr.unlock(t)
		if status != st.OK {
			return status
		}
	}
	// Create exclusive lock file.
	status = util.CreateAndWrite(t.Path+t.Name+".exclusive", tr.ID)
	if status != st.OK {
		return status
	}
	tr.Locked = append(tr.Locked[:], t)
	return st.OK
}

func (tr *Transaction) SLock(t *table.Table) int {
	existingLocks, status := LocksOf(t)
	if status != st.OK {
		return status
	}
	// Do not lock if another transaction has locked the table exclusively.
	if existingLocks.Exclusive != 0 && existingLocks.Exclusive != tr.id {
		return st.CannotLockInShared
	}
	// Release previously acquired exclusive lock, if any.
	if existingLocks.Exclusive == tr.id {
		status = tr.unlock(t)
		if status != st.OK {
			return status
		}
	}
	// Create shared lock file.
	status = util.CreateAndWrite(t.Path+t.Name+".shared/"+tr.ID, "")
	if status != st.OK {
		return status
	}
	tr.Locked = append(tr.Locked[:], t)
	return st.OK
}

// Releases locks acquired by this transaction on the table.
func (tr *Transaction) unlock(t *table.Table) int {
	existingLocks, status := LocksOf(t)
	if status != st.OK {
		return status
	}
	// Release exclusive lock by deleting the exclusive lock file.
	if existingLocks.Exclusive == tr.id {
		err := os.Remove(t.Path + t.Name + ".exclusive")
		if err != nil {
			return st.CannotUnlockExclusiveLock
		}
		return st.OK
	}
	// Release shared lock by deleting the shared lock file.
	for _, lock := range existingLocks.Shared {
		if lock == tr.id {
			err := os.Remove(t.Path + t.Name + ".shared/" + tr.ID)
			if err != nil {
				return st.CannotUnlockSharedLock
			}
		}
	}
	return st.OK
}

// Attempts to lock all tables in the database exclusively.
func (tr *Transaction) LockAll() int {
	for _, table := range tr.DB.Tables {
		status := tr.ELock(table)
		if status != st.OK {
			return status
		}
	}
	return st.OK
}
