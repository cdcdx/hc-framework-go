package repository

import (
	"context"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/model"
)

func newOutboxRepo(t *testing.T) *PointsOutboxRepository {
	t.Helper()
	gdb := openRepoDB(t, &model.PointsOutbox{})
	return NewPointsOutboxRepository(gdb)
}

// TestPointsOutbox_ClaimIdempotent 验证同 eventID 仅一个竞争者能认领（恰好一次语义）。
func TestPointsOutbox_ClaimIdempotent(t *testing.T) {
	repo := newOutboxRepo(t)
	if err := repo.db.Create(&model.PointsOutbox{UserID: "u1", Delta: 10, RefType: "task", EventID: "evt-1", Status: model.OutboxStatusPending}).Error; err != nil {
		t.Fatal(err)
	}

	claimed, err := repo.Claim(context.Background(), "evt-1")
	if err != nil || !claimed {
		t.Fatalf("first claim: claimed=%v err=%v, want true", claimed, err)
	}
	// 第二次认领同一 eventID（已是 processing）必须失败，防止重复应用积分。
	claimed2, err := repo.Claim(context.Background(), "evt-1")
	if err != nil || claimed2 {
		t.Fatalf("second claim: claimed=%v err=%v, want false", claimed2, err)
	}
}

// TestPointsOutbox_MarkDone 验证 processing→done 后不可再被认领。
func TestPointsOutbox_MarkDone(t *testing.T) {
	repo := newOutboxRepo(t)
	if err := repo.db.Create(&model.PointsOutbox{UserID: "u1", EventID: "evt-2", Status: model.OutboxStatusPending}).Error; err != nil {
		t.Fatal(err)
	}

	if _, err := repo.Claim(context.Background(), "evt-2"); err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkDone(context.Background(), "evt-2"); err != nil {
		t.Fatal(err)
	}
	// 已 done，无法再次认领。
	if claimed, _ := repo.Claim(context.Background(), "evt-2"); claimed {
		t.Fatal("should not claim a done record")
	}
}

// TestPointsOutbox_Requeue 验证 processing→pending 回退后可由 relay 重新认领。
func TestPointsOutbox_Requeue(t *testing.T) {
	repo := newOutboxRepo(t)
	if err := repo.db.Create(&model.PointsOutbox{UserID: "u1", EventID: "evt-2b", Status: model.OutboxStatusPending}).Error; err != nil {
		t.Fatal(err)
	}

	if _, err := repo.Claim(context.Background(), "evt-2b"); err != nil {
		t.Fatal(err)
	}
	// 应用失败回退 pending，交由 relay 重试（Requeue 仅对 processing 态生效）。
	if err := repo.Requeue(context.Background(), "evt-2b"); err != nil {
		t.Fatal(err)
	}
	if claimed, _ := repo.Claim(context.Background(), "evt-2b"); !claimed {
		t.Fatal("should re-claim after requeue")
	}
}

// TestPointsOutbox_PendingOlderThan 验证 relay 仅拉取早于 grace 窗口的 pending 记录。
func TestPointsOutbox_PendingOlderThan(t *testing.T) {
	repo := newOutboxRepo(t)
	if err := repo.db.Create(&model.PointsOutbox{UserID: "u1", EventID: "evt-3", Status: model.OutboxStatusPending, CreatedAt: time.Now().Add(-time.Hour)}).Error; err != nil {
		t.Fatal(err)
	}

	rows, err := repo.PendingOlderThan(context.Background(), time.Now().Add(-30*time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].EventID != "evt-3" {
		t.Fatalf("PendingOlderThan = %+v, want 1 row evt-3", rows)
	}
}

// TestPointsOutbox_AppendInTx 验证 outbox 记录可在业务事务内追加，提交后可见。
func TestPointsOutbox_AppendInTx(t *testing.T) {
	repo := newOutboxRepo(t)
	tx := repo.db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	if err := repo.AppendInTx(tx, &model.PointsOutbox{UserID: "u1", EventID: "evt-4", Status: model.OutboxStatusPending}); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}

	var count int64
	if err := repo.db.Model(&model.PointsOutbox{}).Where("event_id = ?", "evt-4").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("count after commit = %d, want 1", count)
	}
}
