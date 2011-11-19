package main

import (
	"fmt"
	"database"
	"transaction"
	"constraint"
	"ra"
	"filter"
)

func testRA() {
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

	r := ra.New()
	r.Load(t1)
	r.MultipleSelect(ra.Condition{Alias: "c1", Filter: filter.Gt{}, Parameter: 3},
		ra.Condition{Alias: "c1", Filter: filter.Lt{}, Parameter: 5})
	r.Report()
}

func test() {
	db, err := database.Open("/home/houzuo/temp_db/")
	fmt.Println("Open database?", err)
	t1, _ := db.New("t1")
	tr := transaction.New(db)

	fmt.Println(t1.Add("c1", 5))
	fmt.Println(t1.Add("c2", 5))
	fmt.Println(constraint.PK(db, t1, "c1"))

	fmt.Println(tr.Insert(t1, map[string]string{"c1": "1", "c2": "a"}))
	fmt.Println("X", tr.Insert(t1, map[string]string{"c1": "1", "c2": "a"}))
	fmt.Println(tr.Insert(t1, map[string]string{"c1":"2", "c2":"b"}))
	fmt.Println(tr.Insert(t1, map[string]string{"c1":"3", "c2":"c"}))
	fmt.Println(tr.Insert(t1, map[string]string{"c1":"4", "c2":"d"}))

	t2, _ := db.New("t2")
	fmt.Println(t2.Add("c1", 5))
	fmt.Println(t2.Add("c3", 5))
	fmt.Println(constraint.FK(db, t2, "c1", t1, "c1"))
	fmt.Println("Inserting into t2-------------------------------------")

	fmt.Println(tr.Insert(t2, map[string]string{"c1":"1", "c3":"aa"}))
	
	fmt.Println(tr.Insert(t2, map[string]string{"c1":"2", "c3":"bb"}))
	fmt.Println(tr.Insert(t2, map[string]string{"c1":"3", "c3":"cc"}))
	fmt.Println(tr.Insert(t2, map[string]string{"c1":"4", "c3":"dd"}))
	fmt.Println("------")
	fmt.Println("X", tr.Insert(t2, map[string]string{"c1":"5", "c3":"dd"}))
	fmt.Println("X", tr.Delete(t1, 1))
	
}

func main() {
	testRA()
	//test()
}
