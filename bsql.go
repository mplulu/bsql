package bsql

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/lib/pq"
)

type DB struct {
	*sql.DB
}

type QuerableRow interface {
	QueryRow(query string, args ...interface{}) *sql.Row
}

func Insert(db QuerableRow, tableName string, dict map[string]interface{}) (id int64, err error) {
	var keyBuffer bytes.Buffer
	var valueBuffer bytes.Buffer
	keyBuffer.WriteString(fmt.Sprintf("INSERT INTO %s (", tableName))
	valueBuffer.WriteString(") VALUES (")
	values := []interface{}{}
	var counter int
	for key, value := range dict {
		keyBuffer.WriteString(key)
		valueBuffer.WriteString(fmt.Sprintf("$%d", counter+1))
		if counter != len(dict)-1 {
			keyBuffer.WriteString(", ")
			valueBuffer.WriteString(", ")
		}
		values = append(values, value)
		counter++
	}
	valueBuffer.WriteString(") RETURNING id;")
	keyBuffer.WriteString(valueBuffer.String())
	err = db.QueryRow(keyBuffer.String(), values...).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, err
}

func (db *DB) QueryInt(queryString string, args ...interface{}) (value int, err error) {
	var sqlValue sql.NullInt64
	row := db.QueryRow(queryString, args...)
	err = row.Scan(&sqlValue)
	return int(sqlValue.Int64), err
}

func (db *DB) QueryInt64(queryString string, args ...interface{}) (value int64, err error) {
	var sqlValue sql.NullInt64
	row := db.QueryRow(queryString, args...)
	err = row.Scan(&sqlValue)
	return sqlValue.Int64, err
}

func (db *DB) QueryFloat64(queryString string, args ...interface{}) (value float64, err error) {
	var sqlValue sql.NullFloat64
	row := db.QueryRow(queryString, args...)
	err = row.Scan(&sqlValue)
	return sqlValue.Float64, err
}

func (db *DB) QueryString(queryString string, args ...interface{}) (value string, err error) {
	var sqlValue sql.NullString
	row := db.QueryRow(queryString, args...)
	err = row.Scan(&sqlValue)
	return sqlValue.String, err
}

func (db *DB) ReadSqlToDbIgnoreIfExist(path string) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Printf("error read sql %v\n", err)
		return
	}

	var removedCommentContent bytes.Buffer
	for _, lineString := range strings.Split(string(content), "\n") {
		if strings.Index(lineString, "--") != 0 {
			removedCommentContent.WriteString(lineString)
			removedCommentContent.WriteString("\n")
		}
	}
	queryCount := 0
	for _, queryString := range strings.Split(removedCommentContent.String(), ";\n") {
		queryCount++
		_, err = db.Exec(queryString)
		if err != nil {
			if val, ok := err.(*pq.Error); ok {
				if val.Code.Name() == "unique_violation" || val.Code.Name() == "duplicate_table" || val.Code.Name() == "duplicate_column" {
					continue
				}
				fmt.Printf("error postgres code %v, %v at query %d. Query: %s\n", val.Code, val, queryCount, queryString)
			} else {
				fmt.Printf("error %v at query %d. Query: %s\n", err, queryCount, queryString)
			}
		}
	}
}

func (db *DB) ReadSqlToDb(path string) error {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Printf("error read sql %v\n", err)
		return err
	}
	var removedCommentContent bytes.Buffer
	for _, lineString := range strings.Split(string(content), "\n") {
		if strings.Index(lineString, "--") != 0 {
			removedCommentContent.WriteString(lineString)
			removedCommentContent.WriteString("\n")
		}
	}

	queryCount := 0
	for _, queryString := range strings.Split(removedCommentContent.String(), ";\n") {
		queryCount++
		_, err = db.Exec(queryString)
		if err != nil {
			fmt.Printf("error %s at query %d. Query: %s\n", err.Error(), queryCount, queryString)
			return err
		}
	}
	return nil
}

func (db *DB) DropDatabaseIgnoreIfNotExist(dbName string) error {
	_, err := db.Exec(fmt.Sprintf("DROP DATABASE %s", dbName))
	if err != nil {
		if val, ok := err.(*pq.Error); ok {
			if val.Code.Name() == "invalid_catalog_name" {
				return nil
			}
		}
	}
	return err
}

func (db *DB) CreateDatabaseIgnoreIfExist(dbName, owner string) error {
	_, err := db.Exec(fmt.Sprintf("CREATE DATABASE %s OWNER %s ENCODING 'utf8'", dbName, owner))
	if err != nil {
		if val, ok := err.(*pq.Error); ok {
			if val.Code.Name() == "unique_violation" {
				return nil
			}
		}
	}
	return err
}

func ConnectPostgresqlDatabase(username, password, dbName string) (dbObject *DB, err error) {
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@127.0.0.1/%s?sslmode=disable",
		username,
		password,
		dbName))
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(20)
	db.SetMaxOpenConns(40)
	// Open doesn't open a connection. Validate DSN data:
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}
