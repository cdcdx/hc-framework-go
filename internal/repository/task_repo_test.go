package repository

import (
	"context"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/model"
)

// TestTaskRepo_FindAll 验证仅返回活跃任务。
func TestTaskRepo_FindAll(t *testing.T) {
	gdb := openRepoDB(t, &model.Task{}, &model.UserTaskProgress{}, &model.EventDedup{})
	repo := NewTaskRepositoryWithCache(db.CreateSingleRWDB(gdb), nil)

	gdb.Create(&model.Task{TaskType: "daily", TaskKey: "k1", TaskName: "active1", TargetValue: 1, RewardPoints: 10, IsActive: true})
	gdb.Create(&model.Task{TaskType: "daily", TaskKey: "k2", TaskName: "active2", TargetValue: 1, RewardPoints: 10, IsActive: true})
	inactive := &model.Task{TaskType: "daily", TaskKey: "k3", TaskName: "inactive", TargetValue: 1, RewardPoints: 10, IsActive: false}
	gdb.Create(inactive)
	gdb.Model(&model.Task{}).Where("id = ?", inactive.ID).UpdateColumn("is_active", false)

	all, err := repo.FindAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("FindAll = %d, want 2 (inactive excluded)", len(all))
	}
}

// TestTaskRepo_FindProgress 验证进度查询与不存在返回 nil。
func TestTaskRepo_FindProgress(t *testing.T) {
	gdb := openRepoDB(t, &model.Task{}, &model.UserTaskProgress{}, &model.EventDedup{})
	repo := NewTaskRepositoryWithCache(db.CreateSingleRWDB(gdb), nil)
	task := &model.Task{TaskType: "daily", TaskKey: "k1", TaskName: "t", TargetValue: 5, RewardPoints: 10, IsActive: true}
	gdb.Create(task)
	period := GetCurrentPeriod("daily")
	gdb.Create(&model.UserTaskProgress{UserID: "u1", TaskID: task.ID, Period: period, CurrentProgress: 2, IsCompleted: false})

	got, err := repo.FindProgress(context.Background(), "u1", task.ID, period)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.CurrentProgress != 2 {
		t.Fatalf("FindProgress = %+v, want current 2", got)
	}
	missing, err := repo.FindProgress(context.Background(), "u1", task.ID, "other-period")
	if err != nil {
		t.Fatal(err)
	}
	if missing != nil {
		t.Fatalf("FindProgress(other) = %+v, want nil", missing)
	}
}

// TestTaskRepo_IncrProgress 验证进度累加与达标自动标记完成。
func TestTaskRepo_IncrProgress(t *testing.T) {
	gdb := openRepoDB(t, &model.Task{}, &model.UserTaskProgress{}, &model.EventDedup{})
	repo := NewTaskRepositoryWithCache(db.CreateSingleRWDB(gdb), nil)
	task := &model.Task{TaskType: "daily", TaskKey: "k1", TaskName: "t", TargetValue: 5, RewardPoints: 10, IsActive: true}
	gdb.Create(task)
	period := GetCurrentPeriod("daily")

	if err := repo.IncrProgress(context.Background(), "u1", task, period, 3); err != nil {
		t.Fatal(err)
	}
	p, _ := repo.FindProgress(context.Background(), "u1", task.ID, period)
	if p.CurrentProgress != 3 || p.IsCompleted {
		t.Fatalf("after +3: progress=%d completed=%v, want 3/false", p.CurrentProgress, p.IsCompleted)
	}

	// 再 +3 → 6 >= 5 达标
	if err := repo.IncrProgress(context.Background(), "u1", task, period, 3); err != nil {
		t.Fatal(err)
	}
	p, _ = repo.FindProgress(context.Background(), "u1", task.ID, period)
	if p.CurrentProgress != 6 || !p.IsCompleted {
		t.Fatalf("after +3: progress=%d completed=%v, want 6/true", p.CurrentProgress, p.IsCompleted)
	}
	if p.CompletedAt == nil {
		t.Fatal("completed progress should set CompletedAt")
	}
}

// TestTaskRepo_ClaimReward 验证领取奖励幂等：已领取后再领返回错误。
func TestTaskRepo_ClaimReward(t *testing.T) {
	gdb := openRepoDB(t, &model.Task{}, &model.UserTaskProgress{}, &model.EventDedup{})
	repo := NewTaskRepositoryWithCache(db.CreateSingleRWDB(gdb), nil)
	task := &model.Task{TaskType: "daily", TaskKey: "k1", TaskName: "t", TargetValue: 5, RewardPoints: 10, IsActive: true}
	gdb.Create(task)
	period := GetCurrentPeriod("daily")
	gdb.Create(&model.UserTaskProgress{UserID: "u1", TaskID: task.ID, Period: period, CurrentProgress: 5, IsCompleted: true, IsClaimed: false})

	if err := repo.ClaimReward(context.Background(), "u1", task.ID, period); err != nil {
		t.Fatalf("ClaimReward first: %v", err)
	}
	var p model.UserTaskProgress
	gdb.First(&p, "user_id = ? AND task_id = ? AND period = ?", "u1", task.ID, period)
	if !p.IsClaimed {
		t.Fatal("IsClaimed should be true after claim")
	}
	// 再次领取：已 claimed → RowsAffected=0 → 错误
	if err := repo.ClaimReward(context.Background(), "u1", task.ID, period); err == nil {
		t.Fatal("expected error on duplicate claim")
	}
}
