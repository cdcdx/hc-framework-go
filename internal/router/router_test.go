package router

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/handler"
)

func init() { gin.SetMode(gin.TestMode) }

// newTestRouter 以最小可用配置构造路由（启用 Prometheus /metrics，关闭限流与熔断，
// 不接入真实服务依赖），用于系统端点的集成测试。
func newTestRouter(t *testing.T) *gin.Engine {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	body := `
server:
  port: 8080
  mode: test
metrics:
  enabled: true
  path: /metrics
ratelimit:
  enabled: false
circuit_breaker:
  enabled: false
`
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	mgr, err := config.LoadManager(p)
	if err != nil {
		t.Fatalf("load manager: %v", err)
	}
	deps := &handler.Dependencies{
		Cfg:    mgr.Get(),
		CfgMgr: mgr,
		// 其余服务/仓库依赖在系统端点测试中无需真实实现，保持 nil
	}
	return Setup(deps)
}

// TestRouter_SystemEndpoints 验证健康检查、就绪、Prometheus 指标、缓存统计与配置热更新端点。
func TestRouter_SystemEndpoints(t *testing.T) {
	r := newTestRouter(t)

	// /health
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/health status = %d, want 200", w.Code)
	}
	var health map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &health); err != nil {
		t.Fatalf("decode /health: %v", err)
	}
	if health["status"] == nil {
		t.Errorf("/health missing status field: %s", w.Body.String())
	}

	// /ready
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/ready status = %d, want 200", w.Code)
	}

	// /metrics —— Prometheus exposition 格式
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("/metrics content-type = %q, want text/plain", ct)
	}
	if !strings.Contains(w.Body.String(), "http_requests_total") {
		t.Errorf("/metrics missing http_requests_total:\n%s", w.Body.String())
	}

	// /metrics/cache —— JSON（未配置缓存时返回禁用提示）
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics/cache", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/metrics/cache status = %d, want 200", w.Code)
	}

	// POST /debug/reload —— 配置热更新（配置管理器已注入）
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/debug/reload", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/debug/reload status = %d, want 200", w.Code)
	}
}
