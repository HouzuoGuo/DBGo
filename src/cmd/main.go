package main

import (
	"fmt"
	"database"
)

func main() {
	fmt.Println("Hello GoDB!")
	db := database.OpenDatabase("/home/houzuo/test_db")
	fmt.Println(db)
}
