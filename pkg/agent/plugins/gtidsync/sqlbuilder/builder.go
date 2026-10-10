// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package sqlbuilder

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/schema"
)

// BuildInsertSql 构建插入sql并返回参数
func BuildInsertSql(table *schema.Table, rows []interface{}) (string, []interface{}, error) {
	err := check(table, rows, "insert")
	if err != nil {
		return "", nil, err
	}
	colLength := len(table.Columns)
	colsName := make([]string, colLength)
	placeholders := make([]string, colLength)
	var args []interface{}

	for i := range table.Columns {
		colsName[i] = wrapColName(table.Columns[i].Name)
		placeholders[i] = "?"
		args = append(args, rows[i])
	}
	cols := strings.Join(colsName, ", ")
	values := strings.Join(placeholders, ", ")
	sqlStr := fmt.Sprintf("insert into `%s`.`%s` (%s) values(%s);", table.Schema, table.Name, cols, values)
	return sqlStr, args, nil
}

// BuildDeleteSql 构建删除sql并返回参数
func BuildDeleteSql(table *schema.Table, rows []interface{}) (string, []interface{}, error) {
	err := check(table, rows, "delete")
	if err != nil {
		return "", nil, err
	}
	conditions, args := genCondition(table, rows)
	sqlStr := fmt.Sprintf("delete from `%s`.`%s` where %s limit 1;", table.Schema, table.Name, strings.Join(conditions, " and "))
	return sqlStr, args, nil
}

// BuildUpdateSql 构建更新sql并返回参数
func BuildUpdateSql(table *schema.Table, conditionRow []interface{}, row []interface{}) (string, []interface{}, error) {
	err := check(table, row, "update")
	if err != nil {
		return "", nil, err
	}
	err = check(table, conditionRow, "update")
	if err != nil {
		return "", nil, err
	}

	setAssignments, setArgs := genAssignment(table, row)
	conditions, condArgs := genCondition(table, conditionRow)

	sqlStr := fmt.Sprintf("update `%s`.`%s` set %s where %s limit 1;",
		table.Schema, table.Name, strings.Join(setAssignments, ", "), strings.Join(conditions, " and "))

	args := append(setArgs, condArgs...)
	return sqlStr, args, nil
}

func genAssignment(table *schema.Table, rows []interface{}) ([]string, []interface{}) {
	colLength := len(table.Columns)
	values := make([]string, colLength)
	var args []interface{}
	for i := range table.Columns {
		values[i] = fmt.Sprintf("`%s` = ?", table.Columns[i].Name)
		args = append(args, rows[i])
	}
	return values, args
}

func genCondition(table *schema.Table, rows []interface{}) ([]string, []interface{}) {
	colLength := len(table.Columns)
	values := make([]string, colLength)
	var args []interface{}
	for i := range table.Columns {
		if rows[i] == nil {
			values[i] = fmt.Sprintf("`%s` is null", table.Columns[i].Name)
		} else {
			values[i] = fmt.Sprintf("`%s` = ?", table.Columns[i].Name)
			args = append(args, rows[i])
		}
	}
	return values, args
}

func check(table *schema.Table, rows []interface{}, action string) error {
	colLength := len(table.Columns)
	rowLength := len(rows)
	if colLength != rowLength {
		return fmt.Errorf("字段不一致，跳过生成%s sql cols: %v values: %v", action, table.Columns, rows)
	}
	return nil
}

func wrapColName(colName string) string {
	return "`" + colName + "`"
}
