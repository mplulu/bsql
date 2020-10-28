package bsql

type ConditionRaw struct {
	ConditorStr string
	QueryRaw    string
}

func (c *ConditionRaw) WhereClause(counter int) string {
	return c.QueryRaw
}

func (c *ConditionRaw) Value() []interface{} {
	return []interface{}{}
}

func (c *ConditionRaw) Conditor() string {
	return c.ConditorStr
}
