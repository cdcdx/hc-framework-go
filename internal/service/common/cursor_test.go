package common

import (
	"testing"
)

// item 游标分页测试元素类型。
type item struct {
	ID int64
}

func idOf(i item) int64 { return i.ID }

// TestCursorPage_Empty 验证空切片：返回空、无游标、hasMore=false。
func TestCursorPage_Empty(t *testing.T) {
	rows, cursor, hasMore := CursorPage([]item{}, 10, idOf)
	if len(rows) != 0 {
		t.Errorf("empty slice: want 0 items, got %d", len(rows))
	}
	if cursor != 0 {
		t.Errorf("empty slice: want cursor=0, got %d", cursor)
	}
	if hasMore {
		t.Errorf("empty slice: want hasMore=false, got true")
	}
}

// TestCursorPage_LessThanLimit 验证 rows 少于 limit：返回全部、无游标、hasMore=false。
func TestCursorPage_LessThanLimit(t *testing.T) {
	rows := []item{{ID: 1}, {ID: 2}}
	result, cursor, hasMore := CursorPage(rows, 10, idOf)
	if len(result) != 2 {
		t.Errorf("less than limit: want 2 items, got %d", len(result))
	}
	if cursor != 0 {
		t.Errorf("less than limit: want cursor=0, got %d", cursor)
	}
	if hasMore {
		t.Errorf("less than limit: want hasMore=false, got true")
	}
}

// TestCursorPage_ExactLimit 验证 rows 等于 limit：返回全部、无游标、hasMore=false。
func TestCursorPage_ExactLimit(t *testing.T) {
	rows := []item{{ID: 1}, {ID: 2}, {ID: 3}}
	result, cursor, hasMore := CursorPage(rows, 3, idOf)
	if len(result) != 3 {
		t.Errorf("exact limit: want 3 items, got %d", len(result))
	}
	if cursor != 0 {
		t.Errorf("exact limit: want cursor=0, got %d", cursor)
	}
	if hasMore {
		t.Errorf("exact limit: want hasMore=false, got true")
	}
}

// TestCursorPage_HasMore 验证 rows 多于 limit：截断、返回下一页游标（第 limit 条记录的 ID）、hasMore=true。
func TestCursorPage_HasMore(t *testing.T) {
	rows := []item{{ID: 10}, {ID: 20}, {ID: 30}, {ID: 40}, {ID: 50}}
	result, cursor, hasMore := CursorPage(rows, 3, idOf)
	if len(result) != 3 {
		t.Errorf("has more: want 3 items, got %d", len(result))
	}
	if cursor != 30 {
		t.Errorf("has more: want cursor=30 (ID of 3rd item), got %d", cursor)
	}
	if !hasMore {
		t.Errorf("has more: want hasMore=true, got false")
	}
	if result[0].ID != 10 || result[1].ID != 20 || result[2].ID != 30 {
		t.Errorf("has more: unexpected items: %+v", result)
	}
}

// TestCursorPage_ZeroLimit 验证 limit<=0 时回退 DefaultPageSize。
func TestCursorPage_ZeroLimit(t *testing.T) {
	rows := make([]item, DefaultPageSize+5)
	for i := range rows {
		rows[i] = item{ID: int64(i + 1)}
	}
	result, cursor, hasMore := CursorPage(rows, 0, idOf)
	if len(result) != DefaultPageSize {
		t.Errorf("zero limit fallback: want %d items, got %d", DefaultPageSize, len(result))
	}
	if cursor != int64(DefaultPageSize) {
		t.Errorf("zero limit fallback: want cursor=%d, got %d", DefaultPageSize, cursor)
	}
	if !hasMore {
		t.Errorf("zero limit fallback: want hasMore=true, got false")
	}
}

// TestCursorPage_NegativeLimit 验证负数 limit 同样回退 DefaultPageSize。
func TestCursorPage_NegativeLimit(t *testing.T) {
	rows := make([]item, DefaultPageSize+1)
	for i := range rows {
		rows[i] = item{ID: int64(i + 1)}
	}
	result, cursor, hasMore := CursorPage(rows, -5, idOf)
	if len(result) != DefaultPageSize {
		t.Errorf("negative limit fallback: want %d items, got %d", DefaultPageSize, len(result))
	}
	if !hasMore {
		t.Errorf("negative limit fallback: want hasMore=true, got false")
	}
	_ = cursor
}

// TestCursorPage_OneItem 验证单行数据。
func TestCursorPage_OneItem(t *testing.T) {
	rows := []item{{ID: 42}}
	result, cursor, hasMore := CursorPage(rows, 10, idOf)
	if len(result) != 1 || result[0].ID != 42 {
		t.Errorf("one item: want [42], got %+v", result)
	}
	if cursor != 0 {
		t.Errorf("one item: want cursor=0, got %d", cursor)
	}
	if hasMore {
		t.Errorf("one item: want hasMore=false, got true")
	}
}
