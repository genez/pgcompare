package main

import (
	"encoding/csv"
	"flag"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

var wg sync.WaitGroup

func main() {
	vacuum := flag.Bool("vacuum", false, "Performs a VACUUM ANALYZE before starting")
	flag.Parse()

	db, err := sqlx.Open("postgres", "postgres://postgres:postgres@192.168.0.2/zucchetti?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if *vacuum {
		log.Println("Performing VACUUM ANALYZE...")
		db.MustExec("VACUUM ANALYZE;")
		log.Println("...done")
	}

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
	defer wg.Done()

	f, err := os.Create(tableName + ".csv")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	rows, err := db.Queryx("SELECT * FROM " + tableName)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}

	w.Write(columns)

	for rows.Next() {
		results, err := rows.SliceScan()
		if err != nil {
			log.Fatal(err)
		}

		s := make([]string, len(results))
		for i, x := range results {
			s[i] = ColumnToString(x)
		}
		w.Write(s)
	}
}

func ColumnToString(col interface{}) string {
	switch col.(type) {
	case float64:
		return strconv.FormatFloat(col.(float64), 'f', 6, 64)
	case int64:
		return strconv.FormatInt(col.(int64), 10)
	case bool:
		return strconv.FormatBool(col.(bool))
	case []byte:
		return string(col.([]byte))
	case string:
		return col.(string)
	case time.Time:
		return col.(time.Time).String()
	case nil:
		return "NULL"
	default:
		// Need to handle anything that ends up here
		return "ERROR"
	}
}
