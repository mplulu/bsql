package bsql

import (
	"database/sql"

	"github.com/lib/pq"
)

func IgnoreAlreadyExist(err error) (filteredErr error) {
	if err == nil {
		return nil
	}
	if val, ok := err.(*pq.Error); ok {
		if val.Code.Name() == "unique_violation" || val.Code.Name() == "duplicate_table" || val.Code.Name() == "duplicate_column" {
			return nil
		}
	}
	return err
}

func IsUniqueViolationErr(err error) bool {
	if err == nil {
		return false
	}
	if val, ok := err.(*pq.Error); ok {
		if val.Code.Name() == "unique_violation" {
			return true
		}
	}
	return false
}

func IsNoRowsErr(err error) bool {
	return err == sql.ErrNoRows
}
