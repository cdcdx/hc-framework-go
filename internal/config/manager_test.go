package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return p
}

// baseConfig 提供通过 Validate 所需的最小 server 配置
const baseConfig = `
server:
  port: 8080
  mode: release
`

func TestManagerHotReload(t *testing.T) {
	p := writeTempConfig(t, baseConfig+`
ratelimit:
  enabled: true
  global:
    rate: 100
    burst: 10
cache:
  degrade:
    enabled: true
    read_skip_l2: true
    write_async: false
`)

	m, err := LoadManager(p)
	if err != nil {
		t.Fatalf("LoadManager: %v", err)
	}
	defer m.Close()

	if got := m.Get().RateLimit.Global.Rate; got != 100 {
		t.Fatalf("initial rate = %d, want 100", got)
	}
	if !m.Get().Cache.Degrade.ReadSkipL2 {
		t.Fatalf("initial read_skip_l2 = false, want true")
	}
	if m.Version() != 0 {
		t.Fatalf("initial version = %d, want 0", m.Version())
	}

	// 修改配置文件后重载
	updated := baseConfig + `
ratelimit:
  enabled: true
  global:
    rate: 200
    burst: 20
cache:
  degrade:
    enabled: true
    read_skip_l2: false
    write_async: true
`
	if err := os.WriteFile(p, []byte(updated), 0o644); err != nil {
		t.Fatalf("rewrite config: %v", err)
	}
	if err := m.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if got := m.Get().RateLimit.Global.Rate; got != 200 {
		t.Fatalf("reloaded rate = %d, want 200", got)
	}
	if m.Get().Cache.Degrade.ReadSkipL2 {
		t.Fatalf("reloaded read_skip_l2 = true, want false")
	}
	if !m.Get().Cache.Degrade.WriteAsync {
		t.Fatalf("reloaded write_async = false, want true")
	}
	// 注：LoadManager 注册了文件监听，WriteFile 触发的 OnConfigChange 也可能调用 Reload，
	// 因此不断言精确版本号，只需验证「重载确实发生、版本递增」（>=1）。
	if m.Version() < 1 {
		t.Fatalf("version = %d, want >= 1 (reload should have incremented)", m.Version())
	}
}

// TestManagerReloadInvalidKeepsOld 校验失败时，Reload 不应替换生效配置（保留旧值，版本号不变）。
func TestManagerReloadInvalidKeepsOld(t *testing.T) {
	p := writeTempConfig(t, baseConfig+`
ratelimit:
  enabled: true
  global:
    rate: 100
`)

	m, err := LoadManager(p)
	if err != nil {
		t.Fatalf("LoadManager: %v", err)
	}
	defer m.Close()
	if got := m.Get().RateLimit.Global.Rate; got != 100 {
		t.Fatalf("initial rate = %d, want 100", got)
	}

	// 写入非法配置（server.port 越界）
	bad := `
server:
  port: 0
  mode: release
ratelimit:
  enabled: true
  global:
    rate: 999
`
	if err := os.WriteFile(p, []byte(bad), 0o644); err != nil {
		t.Fatalf("rewrite config: %v", err)
	}
	if err := m.Reload(); err == nil {
		t.Fatalf("Reload with invalid config should fail")
	}
	// 旧配置应保持不变
	if got := m.Get().RateLimit.Global.Rate; got != 100 {
		t.Fatalf("rate after failed reload = %d, want 100 (old kept)", got)
	}
	if m.Version() != 0 {
		t.Fatalf("version after failed reload = %d, want 0 (unchanged)", m.Version())
	}
}

// TestManagerWatchDebounce 验证文件监听去抖：编辑器一次保存常触发多次 fsnotify 事件，
// 且可能先写入非法中间态（port: 0）再写入合法内容。去抖应将窗口内的连续事件合并为一次
// 重载，待文件落定后读取最终合法配置，既不误报警告、也只应用最终正确值。
func TestManagerWatchDebounce(t *testing.T) {
	p := writeTempConfig(t, baseConfig+`
ratelimit:
  enabled: true
  global:
    rate: 100
`)
	m, err := LoadManager(p)
	if err != nil {
		t.Fatalf("LoadManager: %v", err)
	}
	defer m.Close()
	v0 := m.Version()

	// 模拟编辑器连续两次写盘：首次非法（port=0），随后合法。
	bad := `
server:
  port: 0
  mode: release
ratelimit:
  enabled: true
  global:
    rate: 1
`
	good := baseConfig + `
ratelimit:
  enabled: true
  global:
    rate: 300
`
	if err := os.WriteFile(p, []byte(bad), 0o644); err != nil {
		t.Fatalf("write bad: %v", err)
	}
	if err := os.WriteFile(p, []byte(good), 0o644); err != nil {
		t.Fatalf("write good: %v", err)
	}

	// 等待去抖窗口过后真正执行一次性重载
	deadline := time.Now().Add(2 * watchDebounce)
	for m.Version() == v0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if m.Version() == v0 {
		t.Fatalf("debounced reload did not occur within %v", 2*watchDebounce)
	}
	// 去抖合并后应只读到最终合法配置（rate=300），而非中间态的非法/rate=1。
	if got := m.Get().RateLimit.Global.Rate; got != 300 {
		t.Fatalf("rate = %d, want 300 (final valid config applied, transient bad skipped)", got)
	}
}
