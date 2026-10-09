package sqlbuilder

import (
	"reflect"
	"testing"

	"github.com/go-mysql-org/go-mysql/schema"
)

func mockTable() *schema.Table {
	return &schema.Table{
		Schema: "test_db",
		Name:   "test_table",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
			{Name: "username", Type: schema.TYPE_STRING},
			{Name: "status", Type: schema.TYPE_STRING},
		},
	}
}

func TestBuildInsertSql(t *testing.T) {
	table := mockTable()
	// 模拟恶意输入
	maliciousName := "admin'; DROP TABLE users; --"
	rows := []interface{}{1, maliciousName, "active"}

	sqlStr, args, err := BuildInsertSql(table, rows)
	if err != nil {
		t.Fatalf("BuildInsertSql failed: %v", err)
	}

	expectedSql := "insert into `test_db`.`test_table` (`id`, `username`, `status`) values(?, ?, ?);"
	if sqlStr != expectedSql {
		t.Errorf("Expected SQL %s, got %s", expectedSql, sqlStr)
	}

	if len(args) != 3 {
		t.Fatalf("Expected 3 arguments, got %d", len(args))
	}
	if args[1].(string) != maliciousName {
		t.Errorf("Argument mismatch, expected %s, got %v", maliciousName, args[1])
	}
}

func TestBuildDeleteSql(t *testing.T) {
	table := mockTable()
	// 模拟恶意输入和一个 null 值
	maliciousName := "admin\" OR 1=1 --"
	rows := []interface{}{2, maliciousName, nil}

	sqlStr, args, err := BuildDeleteSql(table, rows)
	if err != nil {
		t.Fatalf("BuildDeleteSql failed: %v", err)
	}

	expectedSql := "delete from `test_db`.`test_table` where `id` = ? and `username` = ? and `status` is null limit 1;"
	if sqlStr != expectedSql {
		t.Errorf("Expected SQL %s, got %s", expectedSql, sqlStr)
	}

	if len(args) != 2 {
		t.Fatalf("Expected 2 arguments, got %d", len(args))
	}
	if args[0].(int) != 2 {
		t.Errorf("Expected argument 1 to be 2, got %v", args[0])
	}
	if args[1].(string) != maliciousName {
		t.Errorf("Expected argument 2 to be %s, got %v", maliciousName, args[1])
	}
}

func TestBuildUpdateSql(t *testing.T) {
	table := mockTable()
	// 旧行
	conditionRow := []interface{}{1, "old_name", "inactive"}
	// 新行
	maliciousName := "new_name'; DELETE FROM logs; --"
	newRow := []interface{}{1, maliciousName, "active"}

	sqlStr, args, err := BuildUpdateSql(table, conditionRow, newRow)
	if err != nil {
		t.Fatalf("BuildUpdateSql failed: %v", err)
	}

	expectedSql := "update `test_db`.`test_table` set `id` = ?, `username` = ?, `status` = ? where `id` = ? and `username` = ? and `status` = ? limit 1;"
	if sqlStr != expectedSql {
		t.Errorf("Expected SQL %s, got %s", expectedSql, sqlStr)
	}

	if len(args) != 6 {
		t.Fatalf("Expected 6 arguments, got %d", len(args))
	}

	expectedArgs := []interface{}{1, maliciousName, "active", 1, "old_name", "inactive"}
	for i, v := range expectedArgs {
		if !reflect.DeepEqual(args[i], v) {
			t.Errorf("Expected argument %d to be %v, got %v", i, v, args[i])
		}
	}
}

func TestCheckLengthMismatch(t *testing.T) {
	table := mockTable()
	// 缺少一列
	rows := []interface{}{1, "username"}

	_, _, err := BuildInsertSql(table, rows)
	if err == nil {
		t.Error("Expected error for row length mismatch, got nil")
	}

	_, _, err = BuildDeleteSql(table, rows)
	if err == nil {
		t.Error("Expected error for row length mismatch, got nil")
	}
}
