package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/gin-gonic/gin"
)

func newCaptchaServer(cfg *config.Config) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewCaptchaHandler(cfg)
	r.GET("/api/v1/captcha/config", h.Config)
	return r
}

func getCaptchaResp(t *testing.T, w *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	data, _ := body["data"].(map[string]interface{})
	return data
}

func TestCaptchaHandler_TypeNone(t *testing.T) {
	cfg := &config.Config{}
	cfg.Captcha.Type = "none"
	cfg.Captcha.WhitelistTTL = 5 * time.Minute
	r := newCaptchaServer(cfg)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/captcha/config", nil))

	resp := getCaptchaResp(t, w)
	if resp["type"] != "none" {
		t.Errorf("type = %v", resp["type"])
	}
	if enabled, ok := resp["enabled"].(bool); !ok || enabled {
		t.Error("type=none should have enabled=false")
	}
	if resp["trigger"] == nil {
		t.Error("missing trigger config")
	}
}

func TestCaptchaHandler_TypeTencent(t *testing.T) {
	cfg := &config.Config{}
	cfg.Captcha.Type = "tencent"
	cfg.Captcha.Tencent.AppID = "app123"
	r := newCaptchaServer(cfg)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/captcha/config", nil))

	resp := getCaptchaResp(t, w)
	if resp["type"] != "tencent" {
		t.Errorf("type = %v", resp["type"])
	}
	if resp["app_id"] != "app123" {
		t.Errorf("app_id = %v", resp["app_id"])
	}
	if resp["script_src"] == "" {
		t.Error("missing script_src")
	}
}

func TestCaptchaHandler_TypeRecaptcha(t *testing.T) {
	cfg := &config.Config{}
	cfg.Captcha.Type = "recaptcha"
	cfg.Captcha.ReCAPTCHA.SiteKey = "sk1"
	r := newCaptchaServer(cfg)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/captcha/config", nil))

	resp := getCaptchaResp(t, w)
	if resp["type"] != "recaptcha" {
		t.Errorf("type = %v", resp["type"])
	}
	if resp["site_key"] != "sk1" {
		t.Errorf("site_key = %v", resp["site_key"])
	}
	if resp["script_src"] != "https://www.google.com/recaptcha/api.js" {
		t.Errorf("unexpected script_src: %v", resp["script_src"])
	}
}

func TestCaptchaHandler_WhitelistTTL(t *testing.T) {
	cfg := &config.Config{}
	cfg.Captcha.Type = "turnstile"
	cfg.Captcha.Turnstile.SiteKey = "sk"
	cfg.Captcha.WhitelistTTL = 5 * time.Minute
	r := newCaptchaServer(cfg)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/captcha/config", nil))

	resp := getCaptchaResp(t, w)
	if ttl, ok := resp["whitelist_ttl_seconds"].(float64); !ok || int(ttl) != 300 {
		t.Errorf("whitelist_ttl_seconds = %v, want 300", resp["whitelist_ttl_seconds"])
	}
}
