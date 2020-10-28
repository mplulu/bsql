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
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Exec(query string, args ...interface{}) (sql.Result, error)
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

func InsertNoId(db QuerableRow, tableName string, dict map[string]interface{}) (err error) {
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
	valueBuffer.WriteString(");")
	keyBuffer.WriteString(valueBuffer.String())
	_, err = db.Exec(keyBuffer.String(), values...)
	if err != nil {
		return err
	}
	return err
}

func (db *DB) Insert(tableName string, dict map[string]interface{}) (id int64, err error) {
	return Insert(db, tableName, dict)
}

func (db *DB) InsertNoId(tableName string, dict map[string]interface{}) (err error) {
	return InsertNoId(db, tableName, dict)
}

func Update(db QuerableRow, tableName string, conditionDict map[string]interface{}, dict map[string]interface{}) (err error) {
	var updateBuffer bytes.Buffer
	updateBuffer.WriteString(fmt.Sprintf("UPDATE %s SET ", tableName))
	values := []interface{}{}
	var counter int
	var counterData int
	for key, value := range dict {
		updateBuffer.WriteString(fmt.Sprintf("%s = $%d", key, counter+1))
		if counterData != len(dict)-1 {
			updateBuffer.WriteString(", ")
		} else {
			updateBuffer.WriteString(" ")
		}
		values = append(values, value)
		counter++
		counterData++
	}

	if len(conditionDict) > 0 {
		updateBuffer.WriteString("WHERE ")
		var counterCond int
		for key, value := range conditionDict {
			updateBuffer.WriteString(fmt.Sprintf("%s = $%d", key, counter+1))
			if counterCond != len(conditionDict)-1 {
				updateBuffer.WriteString(" AND ")
			}
			values = append(values, value)
			counter++
			counterCond++
		}
	}

	_, err = db.Exec(updateBuffer.String(), values...)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) Update(tableName string, conditionDict map[string]interface{}, dict map[string]interface{}) (err error) {
	return Update(db, tableName, conditionDict, dict)
}

func (db *DB) QueryInt(queryString string, args ...interface{}) (value int, err error) {
	var sqlValue sql.NullInt64
	row := db.QueryRow(queryString, args...)
	err = row.Scan(&sqlValue)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return int(sqlValue.Int64), err
}

func (db *DB) QueryInt64(queryString string, args ...interface{}) (value int64, err error) {
	var sqlValue sql.NullInt64
	row := db.QueryRow(queryString, args...)
	err = row.Scan(&sqlValue)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return sqlValue.Int64, err
}

func (db *DB) QueryFloat64(queryString string, args ...interface{}) (value float64, err error) {
	var sqlValue sql.NullFloat64
	row := db.QueryRow(queryString, args...)
	err = row.Scan(&sqlValue)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return sqlValue.Float64, err
}

func (db *DB) QueryString(queryString string, args ...interface{}) (value string, err error) {
	var sqlValue sql.NullString
	row := db.QueryRow(queryString, args...)
	err = row.Scan(&sqlValue)
	if err == sql.ErrNoRows {
		return "", nil
	}
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

func ConnectPostgresqlDatabase(host, username, password, dbName string) (dbObject *DB, err error) {
	if host == "" {
		host = "127.0.0.1"
	}
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		username,
		password,
		host,
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
