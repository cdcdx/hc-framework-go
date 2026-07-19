package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/service/auth"
	"github.com/cdcdx/hc-framework-go/internal/service/common"
	"github.com/gin-gonic/gin"
)

// ───────────────────────── 仓库 / 存储 fake（隔离外部依赖） ─────────────────────────
// 注：fakeLogRepo / fakeMonitorRepo 已在 fixtures_test.go 中定义并复用。

type fakeLoginRepo struct{}

func (fakeLoginRepo) Create(context.Context, *model.LoginRecord) error        { return nil }
func (fakeLoginRepo) CreateBatch(context.Context, []*model.LoginRecord) error { return nil }
func (fakeLoginRepo) Close() error                                            { return nil }
func (fakeLoginRepo) SQLDB() (*sql.DB, error)                                 { return nil, nil }

type fakeLockStore struct {
	mu     sync.Mutex
	locked map[string]time.Time
}

func newFakeLockStore() *fakeLockStore { return &fakeLockStore{locked: map[string]time.Time{}} }

func (f *fakeLockStore) GetFailureCount(context.Context, string) (int, error) { return 0, nil }
func (f *fakeLockStore) RecordFailure(_ context.Context, email string, _ int, _ time.Duration, lockTTL time.Duration) (int, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.locked[email] = time.Now().Add(lockTTL)
	return 1, false, nil
}
func (f *fakeLockStore) GetLockUntil(_ context.Context, email string) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.locked[email], nil
}
func (f *fakeLockStore) Clear(_ context.Context, email string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.locked, email)
	return nil
}

type fakeBlacklist struct {
	mu sync.Mutex
	m  map[string]time.Time
}

func newFakeBlacklist() *fakeBlacklist { return &fakeBlacklist{m: map[string]time.Time{}} }

func (f *fakeBlacklist) AddBlacklist(_ context.Context, jti string, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[jti] = time.Now().Add(ttl)
	return nil
}
func (f *fakeBlacklist) IsBlacklisted(_ context.Context, jti string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.m[jti]
	return ok, nil
}

// fakeProducer 捕获发布的事件类型（验证事件驱动语义）。
type fakeProducer struct {
	mu     sync.Mutex
	events []string
}

func (f *fakeProducer) Send(_ context.Context, msg *event.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, msg.EventType)
	return nil
}
func (f *fakeProducer) SendBatch(_ context.Context, msgs []*event.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range msgs {
		f.events = append(f.events, m.EventType)
	}
	return nil
}
func (f *fakeProducer) SendSync(_ context.Context, msg *event.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, msg.EventType)
	return nil
}
func (f *fakeProducer) Close() error { return nil }
func (f *fakeProducer) has(e string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, x := range f.events {
		if x == e {
			return true
		}
	}
	return false
}

// setupAuthSvc 装配完整 AuthService（内存 fake 依赖），用于 handler 成功路径测试。
func setupAuthSvc(t *testing.T) (*auth.AuthService, *fakeUserRepo, *fakeProducer) {
	t.Helper()
	if err := auth.InitJWT("HS256", "test-secret-key-0123456789", "", "", "hc-framework", time.Hour, 7*24*time.Hour); err != nil {
		t.Fatalf("InitJWT: %v", err)
	}
	cfg := config.NewManager(&config.Config{
		Auth: config.AuthConfig{
			Password: config.PasswordConfig{MinLength: 6},
		},
		Security: config.SecurityConfig{
			AccountLock: config.AccountLockConfig{MaxFailures: 0}, // 关闭锁定，聚焦成功路径
		},
	})
	repo := newFakeUserRepo()
	logSvc := common.NewLogService(&fakeLogRepo{}, &fakeMonitorRepo{}, &fakeLoginRepo{})
	t.Cleanup(logSvc.Close)
	fp := &fakeProducer{}
	svc := auth.NewAuthService(cfg, repo, logSvc, newFakeBlacklist(), fp, newFakeLockStore(), nil)
	return svc, repo, fp
}

// TestAuthHandler_RegisterLoginRefreshChangePassword 覆盖注册/登录/刷新/改密全链路成功路径。
func TestAuthHandler_RegisterLoginRefreshChangePassword(t *testing.T) {
	svc, _, fp := setupAuthSvc(t)
	h := NewAuthHandler(svc)

	// 公开接口引擎
	pub := gin.New()
	pub.POST("/api/v1/auth/register", h.Register)
	pub.POST("/api/v1/auth/login", h.Login)
	pub.POST("/api/v1/auth/refresh", h.RefreshToken)

	// 客户端预哈希约定：64 位 hex 作为密码
	pw := strings.Repeat("a", 64)
	newPw := strings.Repeat("b", 64)

	// 1) 注册
	regBody, _ := json.Marshal(map[string]string{"email": "test@example.com", "password": pw, "nickname": "tester"})
	w := httptest.NewRecorder()
	pub.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", strings.NewReader(string(regBody))))
	if w.Code != http.StatusOK {
		t.Fatalf("register status=%d body=%s", w.Code, w.Body.String())
	}
	var reg map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &reg)
	data := reg["data"].(map[string]interface{})
	accessToken := data["access_token"].(string)
	refreshToken := data["refresh_token"].(string)
	userID := data["user"].(map[string]interface{})["user_id"].(string)
	if accessToken == "" || refreshToken == "" || userID == "" {
		t.Fatal("register returned empty token/user_id")
	}
	if !fp.has(event.EventUserRegistered) {
		t.Fatal("EventUserRegistered not published on register")
	}

	// 2) 登录
	loginBody, _ := json.Marshal(map[string]string{"email": "test@example.com", "password": pw})
	w2 := httptest.NewRecorder()
	pub.ServeHTTP(w2, httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(string(loginBody))))
	if w2.Code != http.StatusOK {
		t.Fatalf("login status=%d body=%s", w2.Code, w2.Body.String())
	}
	if !fp.has(event.EventUserLoggedIn) {
		t.Fatal("EventUserLoggedIn not published on login")
	}

	// 3) 刷新 token
	refBody, _ := json.Marshal(map[string]string{"refresh_token": refreshToken})
	w3 := httptest.NewRecorder()
	pub.ServeHTTP(w3, httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", strings.NewReader(string(refBody))))
	if w3.Code != http.StatusOK {
		t.Fatalf("refresh status=%d body=%s", w3.Code, w3.Body.String())
	}

	// 4) 改密（需鉴权：注入注册用户的 user_id）
	auth := authedEngine(userID, func(r *gin.Engine) {
		r.PUT("/api/v1/user/password", h.ChangePassword)
	})
	chgBody, _ := json.Marshal(map[string]string{"old_password": pw, "new_password": newPw})
	w4 := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/password", strings.NewReader(string(chgBody)))
	auth.ServeHTTP(w4, req)
	if w4.Code != http.StatusOK {
		t.Fatalf("change password status=%d body=%s", w4.Code, w4.Body.String())
	}

	// 5) 新密码可登录
	loginNew, _ := json.Marshal(map[string]string{"email": "test@example.com", "password": newPw})
	w5 := httptest.NewRecorder()
	pub.ServeHTTP(w5, httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(string(loginNew))))
	if w5.Code != http.StatusOK {
		t.Fatalf("login with new password status=%d body=%s", w5.Code, w5.Body.String())
	}

	// 6) 旧密码已失效（改密后哈希变更）：登录应被拒绝（业务码 10202 密码错误）。
	loginOld, _ := json.Marshal(map[string]string{"email": "test@example.com", "password": pw})
	w6 := httptest.NewRecorder()
	pub.ServeHTTP(w6, httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(string(loginOld))))
	if codeOf(t, w6) != model.CodePasswordWrong {
		t.Fatalf("old password login code = %v, want %d (password wrong)", codeOf(t, w6), model.CodePasswordWrong)
	}
}

// TestAuthHandler_RegisterDuplicate 验证重复注册返回邮箱已注册错误码。
func TestAuthHandler_RegisterDuplicate(t *testing.T) {
	svc, _, _ := setupAuthSvc(t)
	h := NewAuthHandler(svc)
	pub := gin.New()
	pub.POST("/api/v1/auth/register", h.Register)

	pw := strings.Repeat("a", 64)
	body, _ := json.Marshal(map[string]string{"email": "dup@example.com", "password": pw})
	first := httptest.NewRecorder()
	pub.ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", strings.NewReader(string(body))))
	if first.Code != http.StatusOK {
		t.Fatalf("first register status=%d body=%s", first.Code, first.Body.String())
	}
	second := httptest.NewRecorder()
	pub.ServeHTTP(second, httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", strings.NewReader(string(body))))
	if codeOf(t, second) != model.CodeEmailRegistered {
		t.Fatalf("duplicate register code = %v, want %d", codeOf(t, second), model.CodeEmailRegistered)
	}
}
