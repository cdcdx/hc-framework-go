package repository

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/cdcdx/hc-framework-go/internal/model"
)

func openRepoDB(t *testing.T, models ...interface{}) *gorm.DB {
	t.Helper()
	re := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	dsn := fmt.Sprintf("file:repo_%s_%s?mode=memory&cache=shared", re.ReplaceAllString(t.Name(), "_"), "db")
	gdb, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if sqlDB, err := gdb.DB(); err == nil {
		sqlDB.SetMaxOpenConns(1)
	}
	if err := gdb.AutoMigrate(models...); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return gdb
}

// TestUserRepo_CreateAndFindByID 验证创建与按 ID 查询（含不存在返回 nil）。
func TestUserRepo_CreateAndFindByID(t *testing.T) {
	gdb := openRepoDB(t, &model.User{})
	repo := NewUserRepository(gdb)

	u := &model.User{UserID: "u1", Username: "alice", Email: "a@x.com", Status: "active"}
	if err := repo.Create(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	got, err := repo.FindByID(context.Background(), "u1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.UserID != "u1" {
		t.Fatalf("FindByID = %+v, want u1", got)
	}
	// 不存在
	missing, err := repo.FindByID(context.Background(), "nope")
	if err != nil {
		t.Fatal(err)
	}
	if missing != nil {
		t.Fatalf("FindByID(missing) = %+v, want nil", missing)
	}
}

// TestUserRepo_FindByEmail 验证按邮箱查询。
func TestUserRepo_FindByEmail(t *testing.T) {
	gdb := openRepoDB(t, &model.User{})
	repo := NewUserRepository(gdb)
	repo.Create(context.Background(), &model.User{UserID: "u1", Username: "alice", Email: "a@x.com", Status: "active"})

	got, err := repo.FindByEmail(context.Background(), "a@x.com")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.UserID != "u1" {
		t.Fatalf("FindByEmail = %+v, want u1", got)
	}
}

// TestUserRepo_Update 验证更新后落库生效。
func TestUserRepo_Update(t *testing.T) {
	gdb := openRepoDB(t, &model.User{})
	repo := NewUserRepository(gdb)
	repo.Create(context.Background(), &model.User{UserID: "u1", Username: "alice", Email: "a@x.com", Status: "active"})

	u, _ := repo.FindByID(context.Background(), "u1")
	u.Username = "alice2"
	if err := repo.Update(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	got, _ := repo.FindByID(context.Background(), "u1")
	if got.Username != "alice2" {
		t.Fatalf("username = %q, want alice2", got.Username)
	}
}

// TestUserRepo_UpdatePoints 验证积分原子增减与余额下限保护。
func TestUserRepo_UpdatePoints(t *testing.T) {
	gdb := openRepoDB(t, &model.User{})
	repo := NewUserRepository(gdb)
	repo.Create(context.Background(), &model.User{UserID: "u1", Username: "alice", Email: "a@x.com", PointsBalance: 100, Status: "active"})

	if err := repo.UpdatePoints(context.Background(), "u1", 50); err != nil {
		t.Fatal(err)
	}
	u, _ := repo.FindByID(context.Background(), "u1")
	if u.PointsBalance != 150 {
		t.Fatalf("balance = %d, want 150", u.PointsBalance)
	}

	// 扣减超过余额：受 check(points_balance >= 0) 约束，RowsAffected=0 → 返回错误
	if err := repo.UpdatePoints(context.Background(), "u1", -200); err == nil {
		t.Fatal("expected error when deducting below balance, got nil")
	}
}
