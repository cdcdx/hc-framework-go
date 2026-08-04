package logic

import (
	"context"
	"testing"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
)

// setupAuthTestCtx 构建带 sqlite in-memory DB 的最小 ServiceContext（用于注册/登录测试）。
func setupAuthTestCtx(t *testing.T) *svc.ServiceContext {
	t.Helper()
	svcCtx := newTestSvc(t)
	// 确保 users 表已建（setupSettleTestCtx 可能不包含 User 表）
	_ = svcCtx.Db.AutoMigrate(&model.User{})
	return svcCtx
}

func TestRegister_SuccessAndDuplicate(t *testing.T) {
	ctx := context.Background()
	svcCtx := setupAuthTestCtx(t)

	// 第一次注册：成功
	req := &hc.RegisterRequest{
		Email:    "test@example.com",
		Password: "password123",
		Username: "tester",
	}
	logic := NewRegisterLogic(ctx, svcCtx)
	resp, err := logic.Register(req)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if resp.AccessToken == "" || resp.RefreshToken == "" {
		t.Fatalf("tokens empty")
	}
	if resp.User.Email != "test@example.com" {
		t.Fatalf("email mismatch: %s", resp.User.Email)
	}

	// 第二次注册（同邮箱）：应报已注册
	_, err = logic.Register(req)
	if errorx.Code(err) != errorx.CodeEmailRegistered {
		t.Fatalf("expected CodeEmailRegistered, got %d", errorx.Code(err))
	}
}

func TestRegister_EmptyFields(t *testing.T) {
	ctx := context.Background()
	svcCtx := setupAuthTestCtx(t)

	cases := []struct {
		name  string
		email string
		pwd   string
	}{
		{"empty email", "", "password123"},
		{"empty password", "a@b.com", ""},
		{"short password", "a@b.com", "123"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := NewRegisterLogic(ctx, svcCtx).Register(&hc.RegisterRequest{
				Email:    c.email,
				Password: c.pwd,
			})
			if errorx.Code(err) != errorx.CodeInvalidParam {
				t.Fatalf("expected CodeInvalidParam, got %d", errorx.Code(err))
			}
		})
	}
}

func TestLogin_SuccessAndWrongPassword(t *testing.T) {
	ctx := context.Background()
	svcCtx := setupAuthTestCtx(t)

	// 先注册
	_, err := NewRegisterLogic(ctx, svcCtx).Register(&hc.RegisterRequest{
		Email:    "login@example.com",
		Password: "correctpassword",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// 正确密码登录
	resp, err := NewLoginLogic(ctx, svcCtx).Login(&hc.LoginRequest{
		Email:    "login@example.com",
		Password: "correctpassword",
	})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if resp.AccessToken == "" {
		t.Fatal("token empty")
	}

	// 错误密码
	_, err = NewLoginLogic(ctx, svcCtx).Login(&hc.LoginRequest{
		Email:    "login@example.com",
		Password: "wrongpassword",
	})
	if errorx.Code(err) != errorx.CodePasswordWrong {
		t.Fatalf("expected CodePasswordWrong, got %d", errorx.Code(err))
	}
}

func TestLogin_UserNotFound(t *testing.T) {
	ctx := context.Background()
	svcCtx := setupAuthTestCtx(t)

	_, err := NewLoginLogic(ctx, svcCtx).Login(&hc.LoginRequest{
		Email:    "nonexist@example.com",
		Password: "anypassword",
	})
	if errorx.Code(err) != errorx.CodePasswordWrong {
		t.Fatalf("expected CodePasswordWrong (no leak), got %d", errorx.Code(err))
	}
}
