package repository

import (
	"context"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/model"
)

// TestEventDedup_MarkIdempotent 验证同一 event_id 在事务内仅首次 Mark 成功（幂等）。
func TestEventDedup_MarkIdempotent(t *testing.T) {
	gdb := openRepoDB(t, &model.EventDedup{})
	repo := NewEventDedupRepository(nil) // Mark 走调用方事务，不依赖 rw

	tx := gdb.Begin()
	first, err := repo.Mark(tx, "evt-x", "idle.settled")
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	tx.Commit()
	if !first {
		t.Fatal("first Mark should return true")
	}

	// 重复 Mark 同 eventID 必须在事务内唯一冲突 → false（effectively-once）。
	tx2 := gdb.Begin()
	second, err := repo.Mark(tx2, "evt-x", "idle.settled")
	if err != nil {
		tx2.Rollback()
		t.Fatal(err)
	}
	tx2.Rollback()
	if second {
		t.Fatal("duplicate Mark should return false")
	}
}

// TestEventDedup_MarkEmptyEventID 验证空 event_id 不插入，直接返回 false。
func TestEventDedup_MarkEmptyEventID(t *testing.T) {
	gdb := openRepoDB(t, &model.EventDedup{})
	repo := NewEventDedupRepository(nil)
	tx := gdb.Begin()
	defer tx.Rollback()

	ok, err := repo.Mark(tx, "", "src")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("empty eventID should return false without insert")
	}
}

// TestEventDedup_DeleteBefore 验证按 created_at 过期清理去重记录。
func TestEventDedup_DeleteBefore(t *testing.T) {
	gdb := openRepoDB(t, &model.EventDedup{})
	rw := db.NewRWDBFromGORM(gdb)
	repo := NewEventDedupRepository(rw)

	tx := gdb.Begin()
	repo.Mark(tx, "evt-old", "src")
	tx.Commit()

	// 删除 1 小时前的记录：刚插入的（created_at≈now）应被删除。
	deleted, err := repo.DeleteBefore(context.Background(), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("DeleteBefore(future) = %d, want 1", deleted)
	}
	// 再次删除 1 小时前的记录：已清空，应为 0。
	deleted2, err := repo.DeleteBefore(context.Background(), time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if deleted2 != 0 {
		t.Fatalf("DeleteBefore(past) = %d, want 0", deleted2)
	}
}
