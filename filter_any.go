package bsql

import (
	"bytes"
	"fmt"
)

type ConditionAny struct {
	ConditorStr string
	Key         string
	ValueRaw    interface{}
}

func (c *ConditionAny) WhereClause(counter int) string {
	return fmt.Sprintf("%s = ANY($%d) \n", c.Key, counter)
}

func (c *ConditionAny) Value() []interface{} {
	return []interface{}{c.ValueRaw}
}

func (c *ConditionAny) Conditor() string {
	return c.ConditorStr
}

func ToConditionAnyString(data []string) string {
	if len(data) == 0 {
		return "'{}'"
	}

	rs := bytes.NewBufferString("")
	for index, j := range data {
		if index == 0 {
			rs.WriteString(fmt.Sprintf(`'{%v`, j))
		} else {
			rs.WriteString(fmt.Sprintf(` ,%v`, j))
		}

		if len(data) == index+1 {
			rs.WriteString(fmt.Sprintf(`}'`))
		}
	}

	return rs.String()
}
