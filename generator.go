package bsql

import (
	"bytes"
	"fmt"
)

func GenANDWhereClause(params map[string]interface{}) (string, []interface{}) {
	var whereClause *bytes.Buffer
	whereClause = bytes.NewBufferString("")
	paramCounter := 0
	values := []interface{}{}
	for key, value := range params {
		whereClause.WriteString(fmt.Sprintf("%s = $%d", key, paramCounter+1))
		if paramCounter != len(params)-1 {
			whereClause.WriteString(" AND ")
		}
		values = append(values, value)
		paramCounter++
	}
	return whereClause.String(), values
}
