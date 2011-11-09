package main

import (
	"fmt"
	"database"
)

func main() {
	db, status := database.Open("/home/houzuo/test_db/")
	fmt.Println("Open db?", status)
	t1, _ := db.New("t1")
		fmt.Println(t1.Add("c1", 5))
		fmt.Println(t1.Insert(map[string]string{"c1":"12345"}))
		fmt.Println(t1.Insert(map[string]string{"c1":"23456"}))
		fmt.Println(t1.Insert(map[string]string{"c1":"34567"}))
	t1.Delete(1)
	t1.Add("c2", 4)
}
