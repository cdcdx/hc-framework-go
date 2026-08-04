package dbclient

import (
	"testing"

	"github.com/cdcdx/hc-framework-go/app/rpc/config"
)

func TestFactory_SQL_Sqlite(t *testing.T) {
	cfgs := config.Databases{
		"business": {Driver: "sqlite", Dsn: "file::memory:?cache=shared", PoolLabel: "biz"},
	}
	f := NewFactory(cfgs)
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
	f := NewFactory(config.Databases{})
	defer f.Close()

	_, err := f.SQL("missing")
	if err == nil {
		t.Fatal("expected error for missing db")
	}
}

func TestFactory_SQL_NotSQL(t *testing.T) {
	cfgs := config.Databases{
		"log": {Driver: "elasticsearch", Elasticsearch: config.ESConfig{Addresses: []string{"http://127.0.0.1:9200"}}},
	}
	f := NewFactory(cfgs)
	defer f.Close()

	_, err := f.SQL("log")
	if err == nil {
		t.Fatal("expected error for NoSQL db via SQL()")
	}
}

func TestFactory_NoSQL_NotImplemented(t *testing.T) {
	tests := []struct{ name, driver string }{
		{"mongodb", "mongodb"},
		{"clickhouse", "clickhouse"},
		{"elasticsearch", "elasticsearch"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Databases{"nosql": {Driver: tc.driver, Dsn: "test://localhost"}}
			f := NewFactory(cfg)
			defer f.Close()

			_, err := f.NoSQL("nosql")
			if err == nil {
				t.Fatal("expected 'not yet implemented' error")
			}
		})
	}
}

func TestFactory_NoSQL_IsSQL(t *testing.T) {
	cfgs := config.Databases{
		"business": {Driver: "sqlite", Dsn: "file::memory:"},
	}
	f := NewFactory(cfgs)
	defer f.Close()

	_, err := f.NoSQL("business")
	if err == nil {
		t.Fatal("expected error when calling NoSQL() on SQL db")
	}
}

func TestFactory_ListSQL(t *testing.T) {
	cfgs := config.Databases{
		"business": {Driver: "sqlite", Dsn: "file::memory:"},
	}
	f := NewFactory(cfgs)
	defer f.Close()

	_, _ = f.SQL("business")
	names := f.ListSQL()
	if len(names) != 1 || names[0] != "business" {
		t.Fatalf("ListSQL = %v, want [business]", names)
	}
}

func TestFactory_Health(t *testing.T) {
	cfgs := config.Databases{
		"business": {Driver: "sqlite", Dsn: "file::memory:"},
	}
	f := NewFactory(cfgs)
	defer f.Close()

	_, _ = f.SQL("business")
	if err := f.Health(); err != nil {
		t.Fatalf("Health: %v", err)
	}
}

func TestFactory_SQLOrDefault(t *testing.T) {
	f := NewFactory(config.Databases{})
	defer f.Close()

	result := f.SQLOrDefault("missing", nil)
	if result != nil {
		t.Fatal("SQLOrDefault should return nil when default is nil")
	}

	// 用 sqlite memory 做默认
	cfgs := config.Databases{
		"real": {Driver: "sqlite", Dsn: "file::memory:"},
	}
	f2 := NewFactory(cfgs)
	defer f2.Close()
	realDB, _ := f2.SQL("real")
	fallback := f2.SQLOrDefault("missing", realDB)
	if fallback != realDB {
		t.Fatal("SQLOrDefault should return defaultDB")
	}
}
