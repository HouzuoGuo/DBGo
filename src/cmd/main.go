package main

import (
	"fmt"
	"database"
	"transaction"
	"constraint"
)

func test() {
	db, err := database.Open("/home/houzuo/temp_db/")
	fmt.Println("Open database?", err)
	t1, _ := db.New("t1")
	t1.Add("c1", 5)
	t1.Insert(map[string]string{"c1": "1"})
	t1.Insert(map[string]string{"c1": "2"})
	t1.Insert(map[string]string{"c1": "3"})
	t1.Add("c2", 4)
	t1.Insert(map[string]string{"c1": "4"})
	t1.Insert(map[string]string{"c1": "5", "c2": "haha"})

	t2, _ := db.New("t2")
	t2.Add("c1", 5)
	t2.Insert(map[string]string{"c1": "5"})
	t2.Insert(map[string]string{"c1": "4"})
	t2.Insert(map[string]string{"c1": "3"})
	t2.Add("c2", 4)
	t2.Insert(map[string]string{"c1": "2"})
	t2.Insert(map[string]string{"c1": "1", "c2": "haha"})
	
	constraint.PK(db, t1, "c1")
	
	tr := transaction.New(db)
	fmt.Println("Insert:", tr.Insert(t1, map[string]string{"c1":"1"}))
}

func main() {
	fmt.Println("Hello")
	test()
}
