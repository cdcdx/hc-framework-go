package gormx

import (
	"testing"
	"time"
)

func TestOpenWithPool_SQLite(t *testing.T) {
	db, err := OpenWithPool("sqlite", ":memory:", PoolConfig{
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		Label:           "test",
	})
	if err != nil {
		t.Fatalf("OpenWithPool: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB(): %v", err)
	}
	if err := sqlDB.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	// 连接池指标采集 goroutine 不应 panic；给一点时间让其运行一次。
	time.Sleep(50 * time.Millisecond)
}

func TestOpen_UnsupportedDriver(t *testing.T) {
	if _, err := Open("oracle", "x"); err == nil {
		t.Fatal("expected error for unsupported driver")
	}
}
