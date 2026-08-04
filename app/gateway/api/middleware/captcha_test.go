package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/config"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/svc"
	"github.com/cdcdx/hc-framework-go/common/captcha"
)

func TestCaptcha_DisabledPassesThrough(t *testing.T) {
	c := config.Config{}
	c.Captcha.Enabled = false
	svcCtx := &svc.ServiceContext{Config: c, CaptchaProvider: &captcha.NoopVerifier{}}

	m := NewCaptchaMiddleware(svcCtx)
	called := false
	handler := m.Handle(func(w http.ResponseWriter, r *http.Request) { called = true })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{}`))
	handler(rec, req)

	if !called {
		t.Error("disabled captcha should pass through without token")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("code = %d, want 200", rec.Code)
	}
}

func TestCaptcha_EnabledMissingTokenRejected(t *testing.T) {
	c := config.Config{}
	c.Captcha.Enabled = true
	// NoopVerifier 始终 Verify=true，但若请求体无 captcha_token 应先被拦截（fail-closed）。
	svcCtx := &svc.ServiceContext{Config: c, CaptchaProvider: &captcha.NoopVerifier{}}

	m := NewCaptchaMiddleware(svcCtx)
	handler := m.Handle(func(w http.ResponseWriter, r *http.Request) {})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"username":"a"}`))
	handler(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("code = %d, want 403 (missing captcha_token)", rec.Code)
	}
}

func TestCaptcha_EnabledValidTokenPasses(t *testing.T) {
	c := config.Config{}
	c.Captcha.Enabled = true
	svcCtx := &svc.ServiceContext{Config: c, CaptchaProvider: &captcha.NoopVerifier{}}

	m := NewCaptchaMiddleware(svcCtx)
	called := false
	handler := m.Handle(func(w http.ResponseWriter, r *http.Request) { called = true })

	rec := httptest.NewRecorder()
	body := `{"username":"a","captcha_token":"anything-noop-accepts"}`
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(body))
	handler(rec, req)

	if !called {
		t.Error("valid captcha_token should pass through")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("code = %d, want 200", rec.Code)
	}
}
