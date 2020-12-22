package bsql

import (
	"fmt"
	"strings"
)

type ConditionRawParams struct {
	ConditorStr           string
	QueryRaw              string
	ConditionRawParamsKey string
	ValueRaw              []interface{}
}

func (c *ConditionRawParams) WhereClause(counter int) string {
	queryRaw := c.QueryRaw
	paramLength := len(c.ValueRaw)
	for i := 0; i < paramLength; i++ {
		queryRaw = strings.Replace(queryRaw, c.ConditionRawParamsKey, fmt.Sprintf("$%d", counter+i), 1)
	}
	return queryRaw
}

func (c *ConditionRawParams) Value() []interface{} {
	return c.ValueRaw
}

func (c *ConditionRawParams) Conditor() string {
	return c.ConditorStr
}
