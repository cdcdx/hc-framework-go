package dbclient

import (
	"testing"
)

// 注意：本包属于 common 层，测试只使用自有的 Specs 契约，
// 不得 import app/rpc/config（会形成 import cycle）。
// 配置层到 Specs 的适配逻辑由 app/rpc/config 的测试覆盖。

func TestFactory_SQL_Sqlite(t *testing.T) {
	f := NewFactory(Specs{
		"business": {Driver: "sqlite", DSN: "file::memory:?cache=shared", PoolLabel: "biz"},
	})
	defer f.Close()

	db, err := f.SQL("business")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}
	// 第二次应返回缓存实例
	db2, err := f.SQL("business")
	if err != nil {
		t.Fatalf("SQL cached: %v", err)
	}
	if db != db2 {
		t.Fatal("cached instance mismatch")
	}
}

func TestFactory_SQL_NotConfigured(t *testing.T) {
	f := NewFactory(Specs{})
	defer f.Close()

	if _, err := f.SQL("missing"); err == nil {
		t.Fatal("expected error for missing db")
	}
}

func TestFactory_SQL_EmptyDSN(t *testing.T) {
	// 上层 ToSpecs 解析失败时会写入只有 Driver 的 spec，此处应给出明确错误
	f := NewFactory(Specs{"business": {Driver: "sqlite"}})
	defer f.Close()

	if _, err := f.SQL("business"); err == nil {
		t.Fatal("expected error for empty DSN")
	}
}

func TestFactory_SQL_NotSQL(t *testing.T) {
	f := NewFactory(Specs{
		"log": {Driver: "elasticsearch", DSN: "http://127.0.0.1:9200"},
	})
	defer f.Close()

	if _, err := f.SQL("log"); err == nil {
		t.Fatal("expected error for NoSQL db via SQL()")
	}
}

func TestFactory_NoSQL_NotImplemented(t *testing.T) {
	for _, driver := range []string{"mongodb", "clickhouse", "elasticsearch"} {
		t.Run(driver, func(t *testing.T) {
			f := NewFactory(Specs{"nosql": {Driver: driver, DSN: "test://localhost"}})
			defer f.Close()

			if _, err := f.NoSQL("nosql"); err == nil {
				t.Fatal("expected 'not yet implemented' error")
			}
		})
	}
}

func TestFactory_NoSQL_IsSQL(t *testing.T) {
	f := NewFactory(Specs{
		"business": {Driver: "sqlite", DSN: "file::memory:"},
	})
	defer f.Close()

	if _, err := f.NoSQL("business"); err == nil {
		t.Fatal("expected error when calling NoSQL() on SQL db")
	}
}

func TestFactory_ListSQL(t *testing.T) {
	f := NewFactory(Specs{
		"business": {Driver: "sqlite", DSN: "file::memory:"},
	})
	defer f.Close()

	_, _ = f.SQL("business")
	names := f.ListSQL()
	if len(names) != 1 || names[0] != "business" {
		t.Fatalf("ListSQL = %v, want [business]", names)
	}
}

func TestFactory_Health(t *testing.T) {
	f := NewFactory(Specs{
		"business": {Driver: "sqlite", DSN: "file::memory:"},
	})
	defer f.Close()

	_, _ = f.SQL("business")
	if err := f.Health(); err != nil {
		t.Fatalf("Health: %v", err)
	}
}

func TestFactory_SQLOrDefault(t *testing.T) {
	f := NewFactory(Specs{})
	defer f.Close()

	if result := f.SQLOrDefault("missing", nil); result != nil {
		t.Fatal("SQLOrDefault should return nil when default is nil")
	}

	f2 := NewFactory(Specs{
		"real": {Driver: "sqlite", DSN: "file::memory:"},
	})
	defer f2.Close()
	realDB, _ := f2.SQL("real")
	if fallback := f2.SQLOrDefault("missing", realDB); fallback != realDB {
		t.Fatal("SQLOrDefault should return defaultDB")
	}
}

func TestDBSpec_IsSQL(t *testing.T) {
	sqlDrivers := []string{"sqlite", "mysql", "postgres", "postgresql"}
	for _, d := range sqlDrivers {
		if !(DBSpec{Driver: d}).IsSQL() {
			t.Errorf("driver %q should be SQL", d)
		}
	}
	for _, d := range []string{"mongodb", "clickhouse", "elasticsearch", ""} {
		if (DBSpec{Driver: d}).IsSQL() {
			t.Errorf("driver %q should not be SQL", d)
		}
	}
}

// TestFactory_Close_AggregatesErrors 验证 Close 尽力关闭全部连接且返回聚合错误：
// 即使某个连接关闭失败也不提前返回，确保其余连接仍被关闭。
func TestFactory_Close_AggregatesErrors(t *testing.T) {
	f := NewFactory(Specs{
		"biz1": {Driver: "sqlite", DSN: "file::memory:"},
		"biz2": {Driver: "sqlite", DSN: "file::memory:"},
	})
	if _, err := f.SQL("biz1"); err != nil {
		t.Fatalf("SQL biz1: %v", err)
	}
	if _, err := f.SQL("biz2"); err != nil {
		t.Fatalf("SQL biz2: %v", err)
	}

	// 正常关闭：返回 nil，且两库连接均已释放。
	if err := f.Close(); err != nil {
		t.Fatalf("Close returned error on healthy dbs: %v", err)
	}

	// 关闭后 ListSQL 仍保留已创建实例记录（设计如此），但再次打开不应 panic。
	if _, err := f.SQL("biz1"); err != nil {
		t.Fatalf("SQL after close should reopen: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
