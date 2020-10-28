package bsql

import "fmt"

type ConditionAnyReverse struct {
	ConditorStr string
	Key         string
	ValueRaw    interface{}
}

func (c *ConditionAnyReverse) WhereClause(counter int) string {
	return fmt.Sprintf("$%d = ANY(%s) \n", counter, c.Key)
}

func (c *ConditionAnyReverse) Value() []interface{} {
	return []interface{}{c.ValueRaw}
}

func (c *ConditionAnyReverse) Conditor() string {
	return c.ConditorStr
}
