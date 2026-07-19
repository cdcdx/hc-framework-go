package logger

import "testing"

func TestSetLevelInvalid(t *testing.T) {
	Init("info", "json")
	if err := SetLevel("nonsense"); err == nil {
		t.Fatal("expected error for unknown log level")
	}
	if GetLevel() != "info" {
		t.Fatalf("level should remain info after invalid set, got %s", GetLevel())
	}
}

func TestSetLevelValid(t *testing.T) {
	Init("info", "json")
	if err := SetLevel("warn"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if GetLevel() != "warn" {
		t.Fatalf("want warn, got %s", GetLevel())
	}
	// 还原，避免影响其它测试
	_ = SetLevel("info")
}

func TestLNotNull(t *testing.T) {
	// L() 在全局未初始化时应惰性初始化，返回非 nil logger，不 panic。
	l := L()
	if l == nil {
		t.Fatal("L() returned nil logger")
	}
	// 再次调用返回同一全局实例（非 nil）。
	if L() == nil {
		t.Fatal("L() returned nil on second call")
	}
}
