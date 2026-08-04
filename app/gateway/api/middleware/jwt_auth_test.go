package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/config"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/svc"
	"github.com/cdcdx/hc-framework-go/common/jwt"
)

func newJwtSvc(t *testing.T) (*svc.ServiceContext, *jwt.Manager) {
	t.Helper()
	mgr, err := jwt.NewManager("HS256", "test-secret-key-at-least-32-bytes-long", "", "", "hc-test",
		time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	c := config.Config{}
	c.Auth.Jwt.Algorithm = "HS256"
	c.Auth.Jwt.SigningKey = "test-secret-key-at-least-32-bytes-long"
	c.Auth.Jwt.Issuer = "hc-test"
	c.Auth.Jwt.AccessTTL = time.Hour
	return &svc.ServiceContext{Config: c, JwtMgr: mgr}, mgr
}

func TestJwtAuth_MissingHeader(t *testing.T) {
	svcCtx, _ := newJwtSvc(t)
	m := NewJwtAuthMiddleware(svcCtx)
	handled := false
	handler := m.Handle(func(w http.ResponseWriter, r *http.Request) { handled = true })

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", rec.Code)
	}
	if handled {
		t.Error("next should NOT be called without token")
	}
}

func TestJwtAuth_InvalidToken(t *testing.T) {
	svcCtx, _ := newJwtSvc(t)
	m := NewJwtAuthMiddleware(svcCtx)
	handler := m.Handle(func(w http.ResponseWriter, r *http.Request) {})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("Authorization", "Bearer not-a-real-token")
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", rec.Code)
	}
}

func TestJwtAuth_ValidTokenInjectsUserID(t *testing.T) {
	svcCtx, mgr := newJwtSvc(t)
	m := NewJwtAuthMiddleware(svcCtx)

	var gotUID string
	handler := m.Handle(func(w http.ResponseWriter, r *http.Request) {
		gotUID, _ = jwt.UserIDFromContext(r.Context())
	})

	token, err := mgr.GenerateAccessToken("u99", "u99@x.com")
	if err != nil {
		t.Fatalf("gen token: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("code = %d, want 200", rec.Code)
	}
	if gotUID != "u99" {
		t.Errorf("injected userID = %q, want u99", gotUID)
	}
}

func TestExtractBearer(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"Bearer abc123", "abc123"},
		{"Bearer   spaced  ", "spaced"},
		{"Basic xyz", ""},
		{"abc123", ""},
		{"", ""},
	}
	for _, tt := range tests {
		if got := extractBearer(tt.in); got != tt.want {
			t.Errorf("extractBearer(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
