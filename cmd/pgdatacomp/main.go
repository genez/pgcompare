package main

import (
	"bufio"
	"encoding/json"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
	"os"
	"sync"
)

var wg sync.WaitGroup

func main() {

	db, err := sqlx.Open("postgres", "postgres://postgres:postgres@192.168.0.2/zucchetti?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Queryx("SELECT relname FROM pg_stat_user_tables where  n_live_tup between 1 and 500 ORDER BY n_live_tup DESC;")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	tables := make(chan string)
	//8 parallel workers
	for i := 0; i < 8; i++ {
		go func() {
			for table := range tables {
				dumpTable(db, table)
			}
		}()
	}

	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			log.Fatal(err)
		}
		wg.Add(1)
		tables <- tableName
	}
	close(tables)

	wg.Wait()
}

func dumpTable(db *sqlx.DB, tableName string) {
	f, err := os.Create(tableName + ".sql")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	rows, err := db.Queryx("SELECT * FROM " + tableName)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	results := make(map[string]interface{})
	for rows.Next() {
		rows.MapScan(results)
		jsonString, err := json.MarshalIndent(results, "", "   ")
		if err != nil {
			log.Fatal(err)
		}
		w.WriteString(string(jsonString))
	}

	w.Flush()

	wg.Done()
}
