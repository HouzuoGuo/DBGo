package main

import (
	"fmt"
	"database"
	"ra"
	"filter"
)

func test() {
	db, _ := database.Open("/home/houzuo/temp_db/")
	t1, _ := db.New("t1")
	t1.Add("c1", 5)
	t1.Insert(map[string]string{"c1": "1"})
	t1.Insert(map[string]string{"c1": "2"})
	t1.Insert(map[string]string{"c1": "3"})
	t1.Add("c2", 4)
	t1.Insert(map[string]string{"c1": "4"})
	t1.Insert(map[string]string{"c1": "5", "c2": "haha"})
	r := ra.New()
	r.Load(t1)
	r.Select("c1", filter.Gt{}, "3")
	r.Report()
	r.Redefine("c1", "new c1")
	r.Report()
	r.Project()
	r.Report()
	
}

func main() {
	fmt.Print("Hello")
	test()
}
