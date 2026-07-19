package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/service/auth"
	"github.com/gin-gonic/gin"
)

// newNilAuthSvc 构造零值 AuthService 供 handler 边界测试：仅校验参数绑定与鉴权前置，
// 不触达 service 业务逻辑（注册/登录/改密/刷新的成功路径需完整 service 装配，见 auth_service_test.go）。
func newNilAuthSvc() *auth.AuthService { return &auth.AuthService{} }

func TestAuthHandler_Register_InvalidBody(t *testing.T) {
	h := NewAuthHandler(newNilAuthSvc())
	r := authedEngine("", func(r *gin.Engine) { r.POST("/api/v1/auth/register", h.Register) })
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", strings.NewReader("not-json")))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body=%s)", w.Code, w.Body.String())
	}
}

func TestAuthHandler_Login_InvalidBody(t *testing.T) {
	h := NewAuthHandler(newNilAuthSvc())
	r := authedEngine("", func(r *gin.Engine) { r.POST("/api/v1/auth/login", h.Login) })
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAuthHandler_ChangePassword_Unauthorized(t *testing.T) {
	h := NewAuthHandler(newNilAuthSvc())
	r := authedEngine("", func(r *gin.Engine) { r.PUT("/api/v1/user/password", h.ChangePassword) })
	w := httptest.NewRecorder()
	body, _ := json.Marshal(map[string]string{"old_password": "x", "new_password": "y"})
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/api/v1/user/password", strings.NewReader(string(body))))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
	if codeOf(t, w) != model.CodeTokenInvalid {
		t.Fatalf("code = %v, want %d", codeOf(t, w), model.CodeTokenInvalid)
	}
}

func TestAuthHandler_GoogleOAuth_MissingCode(t *testing.T) {
	h := NewAuthHandler(newNilAuthSvc())
	r := authedEngine("", func(r *gin.Engine) { r.POST("/api/v1/auth/google", h.GoogleOAuth) })
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/auth/google", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestAuthHandler_RefreshToken_MissingToken(t *testing.T) {
	h := NewAuthHandler(newNilAuthSvc())
	r := authedEngine("", func(r *gin.Engine) { r.POST("/api/v1/auth/refresh", h.RefreshToken) })
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}
