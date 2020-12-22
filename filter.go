package bsql

import (
	"bytes"
	"fmt"
)

type Condition struct {
	ConditorStr   string
	ComparatorStr string
	Key           string
	ValueRaw      interface{}
}

func (c *Condition) WhereClause(counter int) string {
	return fmt.Sprintf("%s %s $%d \n", c.Key, c.ComparatorStr, counter)
}

func (c *Condition) Value() []interface{} {
	return []interface{}{c.ValueRaw}
}

func (c *Condition) Conditor() string {
	return c.ConditorStr
}

type ConditionGroup struct {
	ConditorStr string
	Group       []Conditionable
}

func (c *ConditionGroup) WhereClause(counter int) string {
	whereClause, _ := generateWhereClause(c.Group, counter)

	return fmt.Sprintf("(%s) \n", whereClause)
}

func (c *ConditionGroup) Value() []interface{} {
	list := []interface{}{}
	for _, conditionable := range c.Group {
		list = append(list, conditionable.Value()...)
	}
	return list
}

func (c *ConditionGroup) Conditor() string {
	return c.ConditorStr
}

type Conditionable interface {
	WhereClause(counter int) string
	Value() []interface{}
	Conditor() string
}

func generateWhereClause(list []Conditionable, startingIndex int) (string, []interface{}) {
	var whereClause *bytes.Buffer
	whereClause = bytes.NewBufferString("")
	values := []interface{}{}
	for index, conditionable := range list {
		if index != 0 {
			whereClause.WriteString(fmt.Sprintf(" %s ", conditionable.Conditor()))
		}
		whereStr := conditionable.WhereClause(startingIndex + len(values))
		whereClause.WriteString(whereStr)
		values = append(values, conditionable.Value()...)
	}
	return whereClause.String(), values
}

func GenerateWhereClause(list []Conditionable) (string, []interface{}) {
	if len(list) == 0 {
		return "", []interface{}{}
	}
	whereClause, values := generateWhereClause(list, 1)
	whereClause = fmt.Sprintf("\n WHERE %s", whereClause)
	return whereClause, values
}

func GenerateWhereClauseNoWHERE(list []Conditionable) (string, []interface{}) {
	if len(list) == 0 {
		return "", []interface{}{}
	}
	whereClause, values := generateWhereClause(list, 1)
	return whereClause, values
}
