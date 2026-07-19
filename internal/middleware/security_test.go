package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/gin-gonic/gin"
)

func setupSecurityGin(mgr *config.Manager) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(SecurityIPLimit(mgr, "register"), SecurityIPLimit(mgr, "login"))
	r.POST("/register", func(c *gin.Context) { c.Status(200) })
	r.POST("/login", func(c *gin.Context) { c.Status(200) })
	return r
}

func newTestCfgMgr(ipLimit config.IPLimitConfig, whitelist []string) *config.Manager {
	cfg := &config.Config{
		Security:  config.SecurityConfig{IPLimit: ipLimit},
		RateLimit: config.RateLimitConfig{Whitelist: whitelist},
	}
	cfg.Server.Mode = "test"
	return config.NewManager(cfg)
}

func TestSecurityIPLimit_WhitelistedBypasses(t *testing.T) {
	mgr := newTestCfgMgr(config.IPLimitConfig{RegisterPerMinute: 1}, []string{"127.0.0.1"})
	r := setupSecurityGin(mgr)

	// 白名单 IP 不受限
	for i := 0; i < 10; i++ {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/register", nil)
		req.RemoteAddr = "127.0.0.1:9999"
		r.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("whitelisted IP got %d at request %d", w.Code, i)
		}
	}
}

func TestSecurityIPLimit_RegisterExceeded(t *testing.T) {
	mgr := newTestCfgMgr(config.IPLimitConfig{RegisterPerMinute: 2}, nil)
	r := setupSecurityGin(mgr)

	// 前 2 次应通过
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/register", nil)
		req.RemoteAddr = "10.0.0.1:12345"
		r.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("request %d: expected 200, got %d", i, w.Code)
		}
	}
	// 第 3 次应被拒绝
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/register", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	r.ServeHTTP(w, req)
	if w.Code != 429 {
		t.Fatalf("expected 429 after exceeding limit, got %d", w.Code)
	}
}

func TestSecurityIPLimit_LoginExceeded(t *testing.T) {
	mgr := newTestCfgMgr(config.IPLimitConfig{LoginPerMinute: 1}, nil)
	r := setupSecurityGin(mgr)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/login", nil)
	req.RemoteAddr = "10.0.0.2:12345"
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatal("first login should pass")
	}

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/login", nil)
	req.RemoteAddr = "10.0.0.2:12345"
	r.ServeHTTP(w, req)
	if w.Code != 429 {
		t.Fatal("second login should be rate limited")
	}
}

func TestSecurityIPLimit_ZeroLimitDisablesMiddleware(t *testing.T) {
	// limit=0 时中间件退化，不做任何限制
	mgr := newTestCfgMgr(config.IPLimitConfig{RegisterPerMinute: 0}, nil)
	r := setupSecurityGin(mgr)

	for i := 0; i < 100; i++ {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/register", nil)
		req.RemoteAddr = "10.0.0.3:12345"
		r.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("zero limit should not restrict: got %d at request %d", w.Code, i)
		}
	}
}

func TestSecurityIPLimit_DifferentIPCannotShared(t *testing.T) {
	mgr := newTestCfgMgr(config.IPLimitConfig{RegisterPerMinute: 1}, nil)
	r := setupSecurityGin(mgr)

	// IP1 用掉唯一配额
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/register", nil)
	req.RemoteAddr = "10.0.0.10:12345"
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatal("IP1 should pass")
	}

	// IP2 应仍有自己的配额
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/register", nil)
	req.RemoteAddr = "10.0.0.11:12345"
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatal("IP2 should have its own quota")
	}
}

func TestSecurityWhitelisted_MatchAndNoMatch(t *testing.T) {
	mgr := newTestCfgMgr(config.IPLimitConfig{}, []string{"10.0.0.1", "192.168.0.0/24"})
	if !securityWhitelisted("10.0.0.1", mgr) {
		t.Fatal("10.0.0.1 should be whitelisted")
	}
	if securityWhitelisted("10.0.0.2", mgr) {
		t.Fatal("10.0.0.2 should NOT be whitelisted")
	}
}
