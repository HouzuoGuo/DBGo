package main

import (
	"fmt"
	"database"
)

func main() {
	fmt.Println("hello")
	db, status := database.Open("/home/houzuo/temp_db/")
	fmt.Println("Open db?", status)
	t1, _ := db.New("t1")
	t1.Add("c1", 5)
	t1.Insert(map[string]string{"c1": "12345"})
	t1.Insert(map[string]string{"c1": "23456"})
	t1.Insert(map[string]string{"c1": "34567"})
	t1.Add("c2", 4)
	t1.Insert(map[string]string{"c1": "45678"})
	t1.Insert(map[string]string{"c1": "56789", "c2": "hahahahahaahahah"})
	t1.Update(4, map[string]string{"c2":"blah blah"})
	t1.Delete(0)
	fmt.Println(t1.Remove("c1"))
	fmt.Println(db.Rename("t1", "new t1"))
	//fmt.Println(db.Remove("new t1"))
}
