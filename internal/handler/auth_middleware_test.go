package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/middleware"
	"github.com/cdcdx/hc-framework-go/pkg/jwt"
	"github.com/cdcdx/hc-framework-go/pkg/response"
	"github.com/gin-gonic/gin"
)

// setupAuthServer 构造一个挂载 Auth 中间件的 gin 引擎，并用真实 jwt.Manager 注入验证函数。
// mgr 为 nil 时不清空已有 validator（供测试降级路径单独调用 middleware.SetJWTValidator(nil)）。
func setupAuthServer(t *testing.T, mgr *jwt.Manager) *gin.Engine {
	t.Helper()
	if mgr != nil {
		middleware.SetJWTValidator(func(token string) (*middleware.JWTClaims, error) {
			claims, err := mgr.ValidateToken(token)
			if err != nil {
				return nil, err
			}
			return &middleware.JWTClaims{
				UserID:           claims.UserID,
				Email:            claims.Email,
				RegisteredClaims: claims.RegisteredClaims,
			}, nil
		})
	}
	r := gin.New()
	r.Use(middleware.Auth(&config.Config{}))
	r.GET("/me", func(c *gin.Context) {
		uid, _ := c.Get("user_id")
		response.Success(c, gin.H{"user_id": uid})
	})
	return r
}

func codeOf(t *testing.T, w *httptest.ResponseRecorder) float64 {
	t.Helper()
	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body %q: %v", w.Body.String(), err)
	}
	return body["code"].(float64)
}

func TestAuthMiddleware_MissingHeader(t *testing.T) {
	srv := setupAuthServer(t, jwtManager(t))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/me", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
	if codeOf(t, w) != 10102 {
		t.Fatalf("code = %v, want 10102 (invalid token)", codeOf(t, w))
	}
}

func TestAuthMiddleware_InvalidToken(t *testing.T) {
	srv := setupAuthServer(t, jwtManager(t))
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/me", nil)
	req.Header.Set("Authorization", "Bearer not-a-valid-token")
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
	if codeOf(t, w) != 10102 {
		t.Fatalf("code = %v, want 10102 (invalid token)", codeOf(t, w))
	}
}

func TestAuthMiddleware_ExpiredToken(t *testing.T) {
	// accessTTL 设为负值，签发即过期
	mgr, _ := jwt.NewManager("HS256", "test-secret", "", "", "hc-framework", -time.Hour, time.Hour)
	srv := setupAuthServer(t, mgr)
	tok, err := mgr.GenerateAccessToken("u1", "u1@x.com")
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/me", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
	if codeOf(t, w) != 10101 {
		t.Fatalf("code = %v, want 10101 (expired)", codeOf(t, w))
	}
}

func TestAuthMiddleware_ValidToken(t *testing.T) {
	srv := setupAuthServer(t, jwtManager(t))
	mgr := jwtManager(t)
	tok, err := mgr.GenerateAccessToken("u1", "u1@x.com")
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/me", nil)
	req.Header.Set("Authorization", "Bearer "+tok) // 同时验证 "Bearer " 前缀解析
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var body map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	data := body["data"].(map[string]interface{})
	if data["user_id"] != "u1" {
		t.Fatalf("user_id = %v, want u1", data["user_id"])
	}
}

func TestAuthMiddleware_ValidatorNilDegradesToAnonymous(t *testing.T) {
	// validator 未注入时，带有（任意）Authorization 头也应降级放行（user_id=anonymous），
	// 不阻断请求。注意：无头时 Auth 会在到达 validator 检查前直接 401，故此处必须带头。
	middleware.SetJWTValidator(nil)
	srv := setupAuthServer(t, nil)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/me", nil)
	req.Header.Set("Authorization", "Bearer whatever")
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (degraded pass-through)", w.Code)
	}
	var body map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if body["data"].(map[string]interface{})["user_id"] != "anonymous" {
		t.Fatalf("expected anonymous user_id, got %v", body["data"])
	}
}

func jwtManager(t *testing.T) *jwt.Manager {
	t.Helper()
	m, err := jwt.NewManager("HS256", "test-secret", "", "", "hc-framework", 15*time.Minute, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}
