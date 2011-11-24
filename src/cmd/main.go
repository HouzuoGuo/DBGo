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

package main

import (
	"fmt"
	"os"
	"database"
	"transaction"
	"constraint"
	"ra"
	"filter"
)

const (
	// must point to an EMPTY directory
	DBPath = "/home/houzuo/temp/"
)

// Cleans up created example database.
func cleanUp() {
	os.RemoveAll(DBPath)
	os.MkdirAll(DBPath, 0777)
}

// Open/flush database, create/rename/delete tables.
func Eg1() {
	db, status := database.Open(DBPath)
	// Status 0 means no error has occured.
	fmt.Println("Open database", status)

	// Create a table "t1".
	_, status = db.Create("t1")
	fmt.Println("Create t1", status)

	// Create a table "t2".
	_, status = db.Create("t2")
	fmt.Println("Create t2", status)

	// Create table "t1" again, there should be an error (duplicated table name).
	_, status = db.Create("t1")
	fmt.Println("Create t1 again (error)", status)

	// Rename table "t1" to "t2", there should be an error (duplicated table name).
	status = db.Rename("t1", "t2")
	fmt.Println("Rename t1 to t2 (error)", status)

	// Rename table "t1" to "t3".
	status = db.Rename("t1", "t3")
	fmt.Println("Rename t1 to t3", status)

	// Drop table "t3".
	status = db.Drop("t3")
	fmt.Println("Drop t3", status)

	// Drop table "tnoexist", there should be an error (table not found) .
	status = db.Drop("tnoexist")
	fmt.Println("Drop tnoexist (error)", status)

	// Flush disk buffer.
	db.Flush()
}

// Add/delete columns.
func Eg2() {
	db, status := database.Open(DBPath)
	fmt.Println("Open database", status)

	// Create a table "t1".
	t1, status := db.Create("t1")
	fmt.Println("Create t1", status)

	// Add column c1, maximum length 10 chars.
	fmt.Println("Add c1", t1.Add("c1", 10))

	// Add column c2, maximum length 20 chars.
	fmt.Println("Add c2", t1.Add("c2", 20))

	// Remove column c2.
	fmt.Println("Remove c2", t1.Remove("c2"))

	// Remove column cnoexist, there should be an error (column not found).
	fmt.Println("Remove cnoexist (error)", t1.Remove("cnoexist"))

	// Add column c1, there should be an error (duplicated column name).
	fmt.Println("Add c1 again (error)", t1.Add("c1", 12345))
}

// Insert/update/delete rows.
func Eg3() {
	db, status := database.Open(DBPath)
	fmt.Println("Open database", status)

	// Create a table "t1".
	t1, status := db.Create("t1")
	fmt.Println("Create t1", status)

	// Add colums c1, c2 with maximum length of 2 and 5 chars.
	fmt.Println("Add c1", t1.Add("c1", 2))
	fmt.Println("Add c2", t1.Add("c2", 5))

	// Begin a transaction.
	tr := transaction.New(db)

	// Lock t1 in exclusive mode.
	fmt.Println("Lock t1 exclusively", tr.ELock(t1))

	// Insert three records
	fmt.Println("Insert", tr.Insert(t1, map[string]string{"c1": "a", "c2": "111"}))
	fmt.Println("Insert", tr.Insert(t1, map[string]string{"c1": "b", "c2": "222"}))

	// Long value is truncated to fit the maximum length of the column.
	fmt.Println("Insert", tr.Insert(t1, map[string]string{"c1": "cccc", "c2": "333"}))

	// Print the table.
	rows, status := t1.SelectAll()
	fmt.Println("Select all rows", status)
	for _, row := range rows {
		fmt.Println(row)
	}

	// Update row 0, set c1 to "dd".
	fmt.Println("Update", tr.Update(t1, 0, map[string]string{"c1": "dd"}))
	// Delete row 1
	fmt.Println("Delete", tr.Delete(t1, 1))

	// Print the table again.
	rows, status = t1.SelectAll()
	fmt.Println("Select all rows", status)
	for _, row := range rows {
		fmt.Println(row)
	}

	// Commit the transaction, release table locks.
	tr.Commit()
}

// Table locks
func Eg4() {
	db, status := database.Open(DBPath)
	fmt.Println("Open database", status)

	// Create a table "t1".
	t1, status := db.Create("t1")
	fmt.Println("Create t1", status)

	// Begin two transactions.
	tr1 := transaction.New(db)
	tr2 := transaction.New(db)

	// tr1 locks t1 exclusively.
	fmt.Println("tr1 lock t1 exclusively", tr1.ELock(t1))

	// tr2 tries to acquire exclusive lock or shared lock on t1 but fails.
	fmt.Println("tr2 tries to lock t1 exclusively (error)", tr2.ELock(t1))
	fmt.Println("tr2 tries to lock t1 in shared (error)", tr2.SLock(t1))

	// tr1 downgrades its lock on t1 to shared lock.
	// (No need to remove the exclusively lock first, downgrade happens automatically)
	fmt.Println("tr1 downgrades to shared lock on t1", tr1.SLock(t1))

	// Now tr2 may acquire shared lock on t1.
	fmt.Println("tr2 tries to lock t1 in shared", tr2.SLock(t1))

	// Print existing locks on t1.
	locks, status := transaction.LocksOf(t1)
	fmt.Println("Existing locks on t1", locks, status)

	// A transaction may not acquire exclusively lock on table if the table has shared lock(s) by other transaction(s).
	fmt.Println("tr1 tries to lock t1 exclusively (error)", tr1.ELock(t1))

	// Both commit and rollback will release their locked tables.
	fmt.Println("tr1 commits", tr1.Commit())
	fmt.Println("tr2 rolls back", tr2.Rollback())
	locks, status = transaction.LocksOf(t1)
	fmt.Println("Existing locks on t1", locks, status)
}

// Transaction commit and rollback.
func Eg5() {
	db, status := database.Open(DBPath)
	fmt.Println("Open database", status)

	// Create a table "t1".
	t1, status := db.Create("t1")
	fmt.Println("Create t1", status)

	// Add colums c1, c2 with maximum length of 2 and 5 chars.
	fmt.Println("Add c1", t1.Add("c1", 2))
	fmt.Println("Add c2", t1.Add("c2", 5))

	// Begin a transaction.
	tr := transaction.New(db)

	// Insert a record.
	fmt.Println("Insert", tr.Insert(t1, map[string]string{"c1": "a", "c2": "111"}))

	// Commit all changes made so far.
	fmt.Println("Commit", tr.Commit())

	// After transaction is committed, it is available for next use.

	// Insert a record, update a record, finally delete a record.
	fmt.Println("Insert", tr.Insert(t1, map[string]string{"c1": "b", "c2": "111"}))
	fmt.Println("Update", tr.Update(t1, 1, map[string]string{"c1": "bbb"}))
	fmt.Println("Delete", tr.Delete(t1, 0))

	// Print the table to show the changes made by the transaction.
	rows, status := t1.SelectAll()
	fmt.Println("Select all rows", status)
	for _, row := range rows {
		fmt.Println(row)
	}

	// Now roll back the transaction.
	fmt.Println("Roll back", tr.Rollback())

	// And print the table again.
	rows, status = t1.SelectAll()
	fmt.Println("Select all rows", status)
	for _, row := range rows {
		fmt.Println(row)
	}
}

// PK and FK constraints.
func Eg6() {
	db, status := database.Open(DBPath)
	fmt.Println("Open database", status)

	// Create a table "PEOPLE".
	PERSON, status := db.Create("PEOPLE")
	fmt.Println("Create PEOPLE", status)

	// Add colums to PEOPLE.
	fmt.Println("Add NAME", PERSON.Add("NAME", 20))
	fmt.Println("Add AGE", PERSON.Add("AGE", 2)) // 0-99

	// Create a table "CONTACT".
	CONTACT, status := db.Create("CONTACT")
	fmt.Println("Create CONTACT", status)

	// Add columns to CONTACT.
	fmt.Println("Add SITE", CONTACT.Add("SITE", 20))
	fmt.Println("Add USERNAME", CONTACT.Add("USERNAME", 30))
	fmt.Println("Add NAME", CONTACT.Add("NAME", 20))

	// Lock all tables because we are modifying multiple tables at once.
	// (This is in fact optional, you may also only lock the tables being modified.)
	tr := transaction.New(db)
	fmt.Println("Lock all", tr.LockAll())

	// Make PERSON.NAME a PK, make CONTACT.NAME a FK.
	constraint.PK(db, PERSON, "NAME")
	constraint.FK(db, CONTACT, "NAME", PERSON, "NAME")

	// Insert three records to PERSON, the second record has duplicated NAME which will return an error.
	fmt.Println("Insert 1", tr.Insert(PERSON, map[string]string{"NAME": "Buzz", "AGE": "18"}))
	fmt.Println("Insert 2 (error)", tr.Insert(PERSON, map[string]string{"NAME": "Buzz", "AGE": "17"}))
	fmt.Println("Insert 3", tr.Insert(PERSON, map[string]string{"NAME": "Nikki", "AGE": "15"}))

	// Insert two records to CONTACT, the second record does not correspond to a NAME in PERSON which will return an error.
	fmt.Println("Insert 1", tr.Insert(CONTACT, map[string]string{"SITE": "Twitter", "USERNAME": "buzz", "NAME": "Buzz"}))
	fmt.Println("Insert 2 (error)", tr.Insert(CONTACT, map[string]string{"SITE": "FB", "USERNAME": "CG", "NAME": "Christina"}))

	// Update "Buzz" in PERSON will trigger update-restricted and will return an error.
	fmt.Println("Update 1 (error)", tr.Update(PERSON, 0, map[string]string{"NAME": "BuzzM"}))

	// Update "Nikki" in PERSON will trigger update-restricted but will not return an error.
	fmt.Println("Update 1", tr.Update(PERSON, 1, map[string]string{"NAME": "NikkiH"}))

	// Delete "Buzz" in PERSON will trigger delete-restricted and will return an error.
	fmt.Println("Delete 1 (error)", tr.Delete(PERSON, 0))

	// Delete "NikkiH" in PERSON will trigger delete-restricted but will not return an error.
	fmt.Println("Delete 2", tr.Delete(PERSON, 1))
	fmt.Println("Commit", tr.Commit())

	// Remove the PK and FK constraints.
	fmt.Println("Remove PK constraint", constraint.RemovePK(db, PERSON, "NAME"))
	fmt.Println("Remove FK constraint", constraint.RemoveFK(db, CONTACT, "NAME", PERSON, "NAME"))
}

// Handle query.
func Eg7() {
	/*
		CREATE TABLE PERSON (
			NAME CHAR(20),
			AGE  CHAR(2)
		);

		CREATE TABLE CONTACT (
			NAME CHAR(20),
			SITE CHAR(20),
			USERNAME CHAR(40)
		);

		INSERT INTO PERSON VALUES('BUZZ', '18');
		INSERT INTO PERSON VALUES('CHRISTINA', '16');
		INSERT INTO PERSON VALUES('JOSHUA', '21');
		INSERT INTO PERSON VALUES('NIKKI', '16');

		INSERT INTO CONTACT VALUES('BUZZ', 'TWITTER', 'BUZZ01');
		INSERT INTO CONTACT VALUES('CHRISTINA', 'FACEBOOK', 'CG');
		INSERT INTO CONTACT VALUES('CHRISTINA', 'SKYPE', 'CGG');
		INSERT INTO CONTACT VALUES('JOSHUA', 'TWITTER', 'JAMD')
		INSERT INTO CONTACT VALUES('NIKKI', 'MYB', 'NH');
		COMMIT;

		SELECT SITE, USERNAME
		FROM CONTACT, PERSON
		WHERE PERSON.NAME = CONTACT.NAME
		AND PERSON.AGE > 17;

		-- NOTE THAT THE FOLLOWING 'TRANSLATION' DOES NOT CHECK FOR ERRORS.
	*/

	db, _ := database.Open(DBPath)
	PERSON, _ := db.Create("PERSON")
	PERSON.Add("NAME", 20)
	PERSON.Add("AGE", 2)

	CONTACT, _ := db.Create("CONTACT")
	CONTACT.Add("NAME", 20)
	CONTACT.Add("SITE", 20)
	CONTACT.Add("USERNAME", 40)

	tr := transaction.New(db)
	tr.Insert(PERSON, map[string]string{"NAME": "BUZZ", "AGE": "18"})
	tr.Insert(PERSON, map[string]string{"NAME": "CHRISTINA", "AGE": "16"})
	tr.Insert(PERSON, map[string]string{"NAME": "JOSHUA", "AGE": "21"})
	tr.Insert(PERSON, map[string]string{"NAME": "NIKKI", "AGE": "16"})

	tr.Insert(CONTACT, map[string]string{"NAME": "BUZZ", "SITE": "TWITTER", "USERNAME": "BUZZ01"})
	tr.Insert(CONTACT, map[string]string{"NAME": "CHRISTINA", "SITE": "FACEBOOK", "USERNAME": "CG"})
	tr.Insert(CONTACT, map[string]string{"NAME": "CHRISTINA", "SITE": "SKYPE", "USERNAME": "CGG"})
	tr.Insert(CONTACT, map[string]string{"NAME": "JOSHUA", "SITE": "TWITTER", "USERNAME": "JAMD"})
	tr.Insert(CONTACT, map[string]string{"NAME": "NIKKI", "SITE": "MYB", "USERNAME": "NH"})
	tr.Commit()

	tr.SLock(PERSON)
	tr.SLock(CONTACT)
	query := ra.New()
	query.Load(PERSON)
	query.NLJoin("NAME", CONTACT, "NAME")
	query.Select("AGE", filter.Gt{}, 17)
	query.Project("SITE", "USERNAME")

	for i := 0; i < query.NumberOfRows(); i++ {
		row, status := query.Read(i)
		fmt.Println(row, status)
	}
	tr.Commit()
}

// Handle UPDATE/DELETE statements.
func Eg8() {
	/*
		CREATE TABLE PERSON (
			NAME CHAR(20),
			AGE  CHAR(2)
		);

		CREATE TABLE CONTACT (
			NAME CHAR(20),
			SITE CHAR(20),
			USERNAME CHAR(40)
		);

		INSERT INTO PERSON VALUES('BUZZ', '18');
		INSERT INTO PERSON VALUES('CHRISTINA', '16');
		INSERT INTO PERSON VALUES('JOSHUA', '21');
		INSERT INTO PERSON VALUES('NIKKI', '16');

		INSERT INTO CONTACT VALUES('BUZZ', 'TWITTER', 'BUZZ01');
		INSERT INTO CONTACT VALUES('CHRISTINA', 'FACEBOOK', 'CG');
		INSERT INTO CONTACT VALUES('CHRISTINA', 'SKYPE', 'CGG');
		INSERT INTO CONTACT VALUES('JOSHUA', 'TWITTER', 'JAMD')
		INSERT INTO CONTACT VALUES('NIKKI', 'MYB', 'NH');
		COMMIT;

		DELETE FROM CONTACT
		WHERE NAME IN 
		(SELECT NAME FROM PERSON WHERE NAME = CONTACT.NAME AND
		AGE > 18);

		UPDATE CONTACT
		SET SITE = "FB"
		WHERE SITE = "FACEBOOK";

		COMMIT;

		SELECT * FROM CONTACT;

		-- NOTE THAT THE FOLLOWING 'TRANSLATION' DOES NOT CHECK FOR ERRORS.
	*/

	db, _ := database.Open(DBPath)
	PERSON, _ := db.Create("PERSON")
	PERSON.Add("NAME", 20)
	PERSON.Add("AGE", 2)

	CONTACT, _ := db.Create("CONTACT")
	CONTACT.Add("NAME", 20)
	CONTACT.Add("SITE", 20)
	CONTACT.Add("USERNAME", 40)

	tr := transaction.New(db)
	tr.Insert(PERSON, map[string]string{"NAME": "BUZZ", "AGE": "18"})
	tr.Insert(PERSON, map[string]string{"NAME": "CHRISTINA", "AGE": "16"})
	tr.Insert(PERSON, map[string]string{"NAME": "JOSHUA", "AGE": "21"})
	tr.Insert(PERSON, map[string]string{"NAME": "NIKKI", "AGE": "16"})

	tr.Insert(CONTACT, map[string]string{"NAME": "JOSHUA", "SITE": "TWITTER", "USERNAME": "CGG"})
	tr.Insert(CONTACT, map[string]string{"NAME": "NIKKI", "SITE": "MYB", "USERNAME": "NH"})
	tr.Insert(CONTACT, map[string]string{"NAME": "BUZZ", "SITE": "TWITTER", "USERNAME": "BUZZ01"})
	tr.Insert(CONTACT, map[string]string{"NAME": "CHRISTINA", "SITE": "FACEBOOK", "USERNAME": "CG"})
	tr.Insert(CONTACT, map[string]string{"NAME": "CHRISTINA", "SITE": "SKYPE", "USERNAME": "JAMD"})
	tr.Commit()

	tr.ELock(PERSON)
	tr.ELock(CONTACT)

	query := ra.New()
	query.Load(PERSON)
	query.NLJoin("NAME", CONTACT, "NAME")
	query.Select("AGE", filter.Gt{}, 18)
	query.Project("SITE", "USERNAME")
	contactResult, _ := query.Table("CONTACT")
	for _, i := range contactResult.RowNumbers {
		tr.Delete(CONTACT, i)
	}

	rows, _ := CONTACT.SelectAll()
	for _, row := range rows {
		fmt.Println(row)
	}

	query2 := ra.New()
	query2.Load(CONTACT)
	query2.Select("SITE", filter.Eq{}, "FACEBOOK")
	contactResult, _ = query.Table("CONTACT")
	for _, i := range contactResult.RowNumbers {
		tr.Update(CONTACT, i, map[string]string{"SITE": "FB"})
	}
	tr.Commit()

	rows, _ = CONTACT.SelectAll()
	for _, row := range rows {
		fmt.Println(row)
	}
}

func main() {
	cleanUp()
	fmt.Println("\n\n\t\tC:")
	Eg1()
	cleanUp()
	fmt.Println("\n\n\t\tC: C:")
	Eg2()
	cleanUp()
	fmt.Println("\n\n\t\tC: C: C:")
	Eg3()
	cleanUp()
	fmt.Println("\n\n\t\tC: C: C: C:")
	Eg4()
	cleanUp()
	fmt.Println("\n\n\t\tC: C: C: C: C:")
	Eg5()
	cleanUp()
	fmt.Println("\n\n\t\tC: C: C: C: C: C:")
	Eg6()
	cleanUp()
	fmt.Println("\n\n\t\tC: C: C: C: C: C: C:")
	Eg7()
	cleanUp()
	fmt.Println("\n\n\t\tC: C: C: C: C: C: C: C:")
	Eg8()
}
