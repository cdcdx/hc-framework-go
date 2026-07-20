package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// TestAdminToken_EmptyToken 验证 Token 为空时所有请求返回 403。
func TestAdminToken_EmptyToken(t *testing.T) {
	cfg := &config.Config{Admin: config.AdminConfig{Token: ""}}
	h := AdminToken(cfg)

	w := performAdminRequest(h, "valid-token")
	if w.Code != http.StatusForbidden {
		t.Errorf("empty token: want 403, got %d", w.Code)
	}
}

// TestAdminToken_WrongToken 验证 Token 不匹配时返回 403。
func TestAdminToken_WrongToken(t *testing.T) {
	cfg := &config.Config{Admin: config.AdminConfig{Token: "secret123"}}
	h := AdminToken(cfg)

	w := performAdminRequest(h, "wrong-token")
	if w.Code != http.StatusForbidden {
		t.Errorf("wrong token: want 403, got %d", w.Code)
	}
}

// TestAdminToken_MissingHeader 验证缺少 X-Admin-Token 请求头时返回 403。
func TestAdminToken_MissingHeader(t *testing.T) {
	cfg := &config.Config{Admin: config.AdminConfig{Token: "secret123"}}
	h := AdminToken(cfg)

	w := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(w)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/admin/test", nil)
	// 不设置 X-Admin-Token 请求头
	h(ctx)
	if w.Code != http.StatusForbidden {
		t.Errorf("missing header: want 403, got %d", w.Code)
	}
}

// TestAdminToken_Success 验证 Token 正确时通过中间件。
func TestAdminToken_Success(t *testing.T) {
	cfg := &config.Config{Admin: config.AdminConfig{Token: "secret123"}}
	h := AdminToken(cfg)

	w := performAdminRequest(h, "secret123")
	if w.Code != http.StatusOK {
		t.Errorf("success: want 200, got %d", w.Code)
	}
}

// performAdminRequest 发送带 X-Admin-Token 请求头的 GET 请求经过中间件。
func performAdminRequest(middleware gin.HandlerFunc, token string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(w)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/admin/test", nil)
	if token != "" {
		ctx.Request.Header.Set("X-Admin-Token", token)
	}
	// 模拟后续 handler：写入 200 表示中间件放行
	next := false
	middleware(ctx)
	if !ctx.IsAborted() {
		next = true
		ctx.Status(http.StatusOK)
	}
	if !next && w.Code == 0 {
		// 中间件已 Abort，无需额外写入
	}
	return w
}
