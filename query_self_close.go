package bsql

import (
	"database/sql"
)

type Rows struct {
	coreRows *sql.Rows
	next     chan bool
	scan     chan bool
}

func (r *Rows) Next() bool {
	return <-r.next
}

func (r *Rows) Scan(dest ...interface{}) error {
	err := r.coreRows.Scan(dest...)
	r.scan <- true
	return err
}

func (db *DB) QuerySelfClose(queryString string, args ...interface{}) (rows *Rows, err error) {
	coreRows, err := db.Query(queryString, args...)
	if err != nil {
		return nil, err
	}

	rows = &Rows{
		coreRows: coreRows,
		next:     make(chan bool),
		scan:     make(chan bool),
	}
	go func() {
		defer coreRows.Close()
		for coreRows.Next() {
			rows.next <- true
			<-rows.scan
		}
		rows.next <- false
	}()
	return rows, nil
}
