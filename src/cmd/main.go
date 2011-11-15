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

	t2, _ := db.New("t2")
	t2.Add("c1", 5)
	t2.Insert(map[string]string{"c1": "5"})
	t2.Insert(map[string]string{"c1": "4"})
	t2.Insert(map[string]string{"c1": "3"})
	t2.Add("c2", 4)
	t2.Insert(map[string]string{"c1": "2"})
	t2.Insert(map[string]string{"c1": "1", "c2": "haha"})

	r := ra.New()
	r.Load(t1)
	r.MultipleSelect(ra.Condition{Alias:"c1", Filter:filter.Gt{}, Parameter:"2"},
					 ra.Condition{Alias:"c1", Filter:filter.Lt{}, Parameter:"4"})
	r.Report()
}

func main() {
	fmt.Print("Hello")
	test()
}
