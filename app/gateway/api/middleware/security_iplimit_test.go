package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/config"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/svc"
)

func newIPLimitSvc(enabled bool, perMinute int) *svc.ServiceContext {
	c := config.Config{}
	c.Security.IPLimitEnabled = enabled
	c.Security.IPLimitPerMinute = perMinute
	return &svc.ServiceContext{Config: c}
}

// 通过 httptest 真正驱动 middleware.Handle 链。
func TestSecurityIPLimit_DefaultLimitWhenZero(t *testing.T) {
	svcCtx := newIPLimitSvc(true, 0) // 0 => 默认 30
	m := NewSecurityIPLimitMiddleware(svcCtx)
	called := 0
	next := func(w http.ResponseWriter, r *http.Request) { called++ }
	handler := m.Handle(next)

	// 发 31 个请求，第 31 个应被拦截（默认上限 30）
	for i := 0; i < 31; i++ {
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		req.RemoteAddr = "1.2.3.4:5555"
		rec := httptest.NewRecorder()
		handler(rec, req)
	}
	if called != 30 {
		t.Errorf("called = %d, want 30 (1 over limit blocked)", called)
	}
}

func TestSecurityIPLimit_ConfiguredLimit(t *testing.T) {
	svcCtx := newIPLimitSvc(true, 2)
	m := NewSecurityIPLimitMiddleware(svcCtx)
	called := 0
	next := func(w http.ResponseWriter, r *http.Request) { called++ }
	handler := m.Handle(next)

	for i := 0; i < 4; i++ {
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		req.RemoteAddr = "9.9.9.9:1234"
		handler(httptest.NewRecorder(), req)
	}
	if called != 2 {
		t.Errorf("called = %d, want 2 (limit 2, rest blocked)", called)
	}
}

func TestSecurityIPLimit_DisabledPassesAll(t *testing.T) {
	svcCtx := newIPLimitSvc(false, 1)
	m := NewSecurityIPLimitMiddleware(svcCtx)
	called := 0
	next := func(w http.ResponseWriter, r *http.Request) { called++ }
	handler := m.Handle(next)

	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		req.RemoteAddr = "5.5.5.5:1"
		handler(httptest.NewRecorder(), req)
	}
	if called != 5 {
		t.Errorf("called = %d, want 5 (limit disabled)", called)
	}
}

func TestClientIP_Precedence(t *testing.T) {
	// X-Forwarded-For 优先取首个
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("X-Forwarded-For", "203.0.113.7, 70.41.3.18, 150.172.238.178")
	r.RemoteAddr = "10.0.0.1:9999"
	if got := clientIP(r); got != "203.0.113.7" {
		t.Errorf("clientIP = %q, want 203.0.113.7", got)
	}

	// 无 XFF 但有 X-Real-IP
	r2 := httptest.NewRequest(http.MethodGet, "/", nil)
	r2.Header.Set("X-Real-IP", "198.51.100.22")
	r2.RemoteAddr = "10.0.0.1:9999"
	if got := clientIP(r2); got != "198.51.100.22" {
		t.Errorf("clientIP = %q, want 198.51.100.22", got)
	}

	// 兜底 RemoteAddr
	r3 := httptest.NewRequest(http.MethodGet, "/", nil)
	r3.RemoteAddr = "192.0.2.5:8080"
	if got := clientIP(r3); got != "192.0.2.5" {
		t.Errorf("clientIP = %q, want 192.0.2.5", got)
	}
}
