package sql

import (
	"github.com/lib/pq"
)

func IgnoreUniqueViolation(err error) (filteredErr error) {
	if err == nil {
		return nil
	}
	if val, ok := err.(*pq.Error); ok {
		if val.Code.Name() == "unique_violation" {
			return nil
		}
	}
	return err
}
