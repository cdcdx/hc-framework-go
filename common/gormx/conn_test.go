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

// TestResolvePoolDefaults 验证连接池兜底规则（不依赖真实 DB 驱动）：
//   - 显式 >0 优先；
//   - mysql/postgres 未配置时兜底为 defaultMax*；
//   - sqlite 未配置时不兜底（返回 0 = 不限制）。
func TestResolvePoolDefaults(t *testing.T) {
	cases := []struct {
		driver   string
		pool     PoolConfig
		wantMax  int
		wantIdle int
		desc     string
	}{
		{"mysql", PoolConfig{}, defaultMaxOpenConns, defaultMaxIdleConns, "mysql 无配置应兜底"},
		{"postgres", PoolConfig{}, defaultMaxOpenConns, defaultMaxIdleConns, "postgres 无配置应兜底"},
		{"sqlite", PoolConfig{}, 0, 0, "sqlite 无配置不兜底"},
		{"mysql", PoolConfig{MaxOpenConns: 80, MaxIdleConns: 20}, 80, 20, "mysql 显式优先"},
		{"sqlite", PoolConfig{MaxOpenConns: 7, MaxIdleConns: 3}, 7, 3, "sqlite 显式优先"},
	}
	for _, c := range cases {
		maxOpen, maxIdle := resolvePoolDefaults(c.driver, c.pool)
		if maxOpen != c.wantMax || maxIdle != c.wantIdle {
			t.Errorf("%s: 期望 (max=%d,idle=%d) 实际 (max=%d,idle=%d)",
				c.desc, c.wantMax, c.wantIdle, maxOpen, maxIdle)
		}
	}
}

// TestOpenWithPool_ExplicitWins 验证 sqlite 显式配置被实际应用（open 后 MaxOpenConnections 生效）。
func TestOpenWithPool_ExplicitWins(t *testing.T) {
	db, err := OpenWithPool("sqlite", ":memory:", PoolConfig{
		MaxOpenConns: 7,
		MaxIdleConns: 3,
		Label:        "test",
	})
	if err != nil {
		t.Fatalf("OpenWithPool: %v", err)
	}
	sqlDB, _ := db.DB()
	if st := sqlDB.Stats(); st.MaxOpenConnections != 7 {
		t.Errorf("sqlite 显式 7 应保留，实际 %d", st.MaxOpenConnections)
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
