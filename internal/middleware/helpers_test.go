package middleware

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// loadTestManager 从 YAML 字符串构建配置管理器（测试辅助）。
// 将 YAML 写入临时文件后通过 config.LoadManager 加载（Validate 仅校验 server 端口/模式）。
func loadTestManager(t *testing.T, yaml string) *config.Manager {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "cfg.yaml")
	if err := os.WriteFile(p, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	mgr, err := config.LoadManager(p)
	if err != nil {
		t.Fatalf("load config manager: %v", err)
	}
	return mgr
}
