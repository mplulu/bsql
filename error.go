package bsql

import (
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
