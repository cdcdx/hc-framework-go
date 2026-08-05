package gormx

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"gorm.io/gorm"
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

func TestIsRecordNotFound(t *testing.T) {
	if IsRecordNotFound(nil) {
		t.Fatal("nil error should not be record-not-found")
	}
	if !IsRecordNotFound(gorm.ErrRecordNotFound) {
		t.Fatal("direct gorm.ErrRecordNotFound must match")
	}
	// gorm 在链路较长时会用 fmt.Errorf 包装 ErrRecordNotFound，
	// 直接 `== gorm.ErrRecordNotFound` 会漏判；IsRecordNotFound 必须兼容。
	wrapped := fmt.Errorf("query failed: %w", gorm.ErrRecordNotFound)
	if !IsRecordNotFound(wrapped) {
		t.Fatal("errors.Is 包装的 ErrRecordNotFound 必须被识别")
	}
	other := errors.New("boom")
	if IsRecordNotFound(other) {
		t.Fatal("无关错误不应被识别为 record-not-found")
	}
}
