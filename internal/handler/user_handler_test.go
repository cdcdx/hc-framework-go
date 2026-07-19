package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/repository"
	"github.com/gin-gonic/gin"
)

// fakeUserRepo 是 repository.UserRepository 的最小内存实现，用于隔离测试 UserHandler。
type fakeUserRepo struct {
	users  map[string]*model.User
	byErr  error // FindByID 返回的错误（模拟 DB 异常）
	updErr error // Update 返回的错误
}

func newFakeUserRepo() *fakeUserRepo {
	return &fakeUserRepo{users: make(map[string]*model.User)}
}

func (f *fakeUserRepo) Create(_ context.Context, u *model.User) error {
	f.users[u.UserID] = u
	return nil
}
func (f *fakeUserRepo) FindByID(_ context.Context, userID string) (*model.User, error) {
	if f.byErr != nil {
		return nil, f.byErr
	}
	if u, ok := f.users[userID]; ok {
		return u, nil
	}
	return nil, nil
}
func (f *fakeUserRepo) FindByEmail(_ context.Context, email string) (*model.User, error) {
	for _, u := range f.users {
		if u.Email == email {
			return u, nil
		}
	}
	return nil, nil
}
func (f *fakeUserRepo) FindByGoogleID(_ context.Context, _ string) (*model.User, error) {
	return nil, nil
}
func (f *fakeUserRepo) Update(_ context.Context, u *model.User) error {
	if f.updErr != nil {
		return f.updErr
	}
	f.users[u.UserID] = u
	return nil
}
func (f *fakeUserRepo) UpdatePoints(_ context.Context, _ string, _ int64) error { return nil }
func (f *fakeUserRepo) UpdatePassword(_ context.Context, userID, passwordHash string, _ interface{}) error {
	if u, ok := f.users[userID]; ok {
		u.PasswordHash = passwordHash
	}
	return nil
}
func (f *fakeUserRepo) AutoMigrate() error      { return nil }
func (f *fakeUserRepo) Close() error            { return nil }
func (f *fakeUserRepo) SQLDB() (*sql.DB, error) { return nil, nil }

// TestUserHandler_Unauthorized 验证未携带有效 user_id 时返回 401。
func TestUserHandler_Unauthorized(t *testing.T) {
	h := NewUserHandler(newFakeUserRepo())
	r := authedEngine("", func(r *gin.Engine) {
		r.GET("/api/v1/user/profile", h.GetProfile)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/user/profile", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
	if codeOf(t, w) != model.CodeTokenInvalid {
		t.Fatalf("code = %v, want %d", codeOf(t, w), model.CodeTokenInvalid)
	}
}

// TestUserHandler_GetProfile_Found 验证存在用户时返回 200 与用户数据。
func TestUserHandler_GetProfile_Found(t *testing.T) {
	repo := newFakeUserRepo()
	repo.Create(context.Background(), &model.User{UserID: "u1", Username: "alice", Email: "a@x.com", PointsBalance: 50, Status: "active"})
	h := NewUserHandler(repo)
	r := authedEngine("u1", func(r *gin.Engine) {
		r.GET("/api/v1/user/profile", h.GetProfile)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/user/profile", nil))
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

// TestUserHandler_GetProfile_NotFound 验证用户不存在时返回 10002。
func TestUserHandler_GetProfile_NotFound(t *testing.T) {
	h := NewUserHandler(newFakeUserRepo())
	r := authedEngine("u1", func(r *gin.Engine) {
		r.GET("/api/v1/user/profile", h.GetProfile)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/user/profile", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (业务码非 0)", w.Code)
	}
	if codeOf(t, w) != model.CodeNotFound {
		t.Fatalf("code = %v, want %d", codeOf(t, w), model.CodeNotFound)
	}
}

// TestUserHandler_GetProfile_DBError 验证仓库异常时返回 10701。
func TestUserHandler_GetProfile_DBError(t *testing.T) {
	repo := newFakeUserRepo()
	repo.byErr = errors.New("db down")
	h := NewUserHandler(repo)
	r := authedEngine("u1", func(r *gin.Engine) {
		r.GET("/api/v1/user/profile", h.GetProfile)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/user/profile", nil))
	if codeOf(t, w) != model.CodeDBError {
		t.Fatalf("code = %v, want %d", codeOf(t, w), model.CodeDBError)
	}
}

// TestUserHandler_UpdateProfile_Success 验证昵称/头像更新成功。
func TestUserHandler_UpdateProfile_Success(t *testing.T) {
	repo := newFakeUserRepo()
	repo.Create(context.Background(), &model.User{UserID: "u1", Username: "alice", Email: "a@x.com", PointsBalance: 50, Status: "active"})
	h := NewUserHandler(repo)
	r := authedEngine("u1", func(r *gin.Engine) {
		r.PUT("/api/v1/user/profile", h.UpdateProfile)
	})
	body, _ := json.Marshal(map[string]interface{}{"nickname": "alice2", "avatar": "http://img/x.png"})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/api/v1/user/profile", strings.NewReader(string(body))))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}
	if repo.users["u1"].Username != "alice2" {
		t.Fatalf("username not updated: %q", repo.users["u1"].Username)
	}
	if repo.users["u1"].AvatarURL != "http://img/x.png" {
		t.Fatalf("avatar not updated: %q", repo.users["u1"].AvatarURL)
	}
}

// TestUserHandler_UpdateProfile_NicknameTooLong 验证昵称超长返回 400。
func TestUserHandler_UpdateProfile_NicknameTooLong(t *testing.T) {
	repo := newFakeUserRepo()
	repo.Create(context.Background(), &model.User{UserID: "u1", Username: "alice", Email: "a@x.com", Status: "active"})
	h := NewUserHandler(repo)
	r := authedEngine("u1", func(r *gin.Engine) {
		r.PUT("/api/v1/user/profile", h.UpdateProfile)
	})
	long := strings.Repeat("x", 51)
	body, _ := json.Marshal(map[string]interface{}{"nickname": long})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/api/v1/user/profile", strings.NewReader(string(body))))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (nickname too long)", w.Code)
	}
}

// TestUserHandler_UpdateProfile_AvatarTooLong 验证头像 URL 超长返回 400。
func TestUserHandler_UpdateProfile_AvatarTooLong(t *testing.T) {
	repo := newFakeUserRepo()
	repo.Create(context.Background(), &model.User{UserID: "u1", Username: "alice", Email: "a@x.com", Status: "active"})
	h := NewUserHandler(repo)
	r := authedEngine("u1", func(r *gin.Engine) {
		r.PUT("/api/v1/user/profile", h.UpdateProfile)
	})
	long := "http://img/" + strings.Repeat("x", 500)
	body, _ := json.Marshal(map[string]interface{}{"avatar": long})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/api/v1/user/profile", strings.NewReader(string(body))))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (avatar too long)", w.Code)
	}
}

// TestUserHandler_UpdateProfile_NotFound 验证用户不存在时返回 10002。
func TestUserHandler_UpdateProfile_NotFound(t *testing.T) {
	h := NewUserHandler(newFakeUserRepo())
	r := authedEngine("u1", func(r *gin.Engine) {
		r.PUT("/api/v1/user/profile", h.UpdateProfile)
	})
	body, _ := json.Marshal(map[string]interface{}{"nickname": "x"})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/api/v1/user/profile", strings.NewReader(string(body))))
	if codeOf(t, w) != model.CodeNotFound {
		t.Fatalf("code = %v, want %d", codeOf(t, w), model.CodeNotFound)
	}
}

// TestUserHandler_GetPoints_Success 验证积分余额接口返回正确余额。
func TestUserHandler_GetPoints_Success(t *testing.T) {
	repo := newFakeUserRepo()
	repo.Create(context.Background(), &model.User{UserID: "u1", Username: "alice", Email: "a@x.com", PointsBalance: 123, Status: "active"})
	h := NewUserHandler(repo)
	r := authedEngine("u1", func(r *gin.Engine) {
		r.GET("/api/v1/user/points", h.GetPoints)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/user/points", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var body map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	data := body["data"].(map[string]interface{})
	if int64(data["points_balance"].(float64)) != 123 {
		t.Fatalf("points_balance = %v, want 123", data["points_balance"])
	}
}

var _ repository.UserRepository = (*fakeUserRepo)(nil)
