package repository

import (
	"context"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/model"
)

// TestLoginRepo_Create 验证单条登录记录的写入与自动主键赋值。
func TestLoginRepo_Create(t *testing.T) {
	gdb := openRepoDB(t, &model.LoginRecord{})
	repo := NewLoginRepository(gdb)

	rec := &model.LoginRecord{
		UserID:      "u1",
		LoginType:   model.LoginTypePassword,
		IPAddress:   "127.0.0.1",
		LoginResult: model.LoginResultSuccess,
	}
	if err := repo.Create(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if rec.ID == 0 {
		t.Fatal("expected auto-increment ID assigned")
	}

	// 读回验证字段落库正确（登录审计字段可靠保存）。
	got := &model.LoginRecord{}
	if err := gdb.First(got, "user_id = ?", "u1").Error; err != nil {
		t.Fatal(err)
	}
	if got.LoginResult != model.LoginResultSuccess {
		t.Fatalf("login_result = %q, want success", got.LoginResult)
	}
}

// TestLoginRepo_CreateBatch 验证批量写入（单条 SQL 多值插入）全部落库。
func TestLoginRepo_CreateBatch(t *testing.T) {
	gdb := openRepoDB(t, &model.LoginRecord{})
	repo := NewLoginRepository(gdb)

	recs := []*model.LoginRecord{
		{UserID: "u1", LoginType: model.LoginTypePassword, LoginResult: model.LoginResultSuccess},
		{UserID: "u2", LoginType: model.LoginTypeGoogle, LoginResult: model.LoginResultFail, FailReason: "oauth_failed"},
		{UserID: "u3", LoginType: model.LoginTypePassword, LoginResult: model.LoginResultFail, FailReason: "password_wrong"},
	}
	if err := repo.CreateBatch(context.Background(), recs); err != nil {
		t.Fatal(err)
	}

	var count int64
	if err := gdb.Model(&model.LoginRecord{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3", count)
	}
}

// TestLoginRepo_CreateBatch_Empty 验证空批次为安全 no-op。
func TestLoginRepo_CreateBatch_Empty(t *testing.T) {
	gdb := openRepoDB(t, &model.LoginRecord{})
	repo := NewLoginRepository(gdb)
	if err := repo.CreateBatch(context.Background(), nil); err != nil {
		t.Fatalf("empty batch should be no-op, got %v", err)
	}
}
