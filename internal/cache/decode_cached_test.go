package cache

import (
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/model"
)

// sample 用于验证「JSON 反序列化中间类型 → 经 JSON 中转还原为 T」的通用能力。
// 带 json tag 以保证 map 键能正确映射回结构体字段。
type sample struct {
	UserID string `json:"user_id"`
	Status string `json:"status"`
}

// TestDecodeCached_Nil 未命中 / 空值缓存：返回类型零值且不报错。
func TestDecodeCached_Nil(t *testing.T) {
	v, err := DecodeCached[*sample](nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != nil {
		t.Fatalf("expected nil, got %+v", v)
	}
}

// TestDecodeCached_DirectGoType 缓存未命中、回源直返的原始 Go 类型：零拷贝直接返回。
func TestDecodeCached_DirectGoType(t *testing.T) {
	rec := &model.IdleRecord{UserID: "u1", Status: "active"}
	v, err := DecodeCached[*model.IdleRecord](rec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v == nil || v.UserID != "u1" || v.Status != "active" {
		t.Fatalf("expected direct return, got %+v", v)
	}
}

// TestDecodeCached_MapMiddleType 缓存命中（L2 JSON 对象）：map[string]interface{} 经 JSON 中转还原。
func TestDecodeCached_MapMiddleType(t *testing.T) {
	m := map[string]interface{}{"user_id": "u1", "status": "active"}
	v, err := DecodeCached[*sample](m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v == nil || v.UserID != "u1" || v.Status != "active" {
		t.Fatalf("expected json transit restore, got %+v", v)
	}
}

// TestDecodeCached_SliceDirect 回源直返的切片：直接返回，不触发 JSON 中转。
func TestDecodeCached_SliceDirect(t *testing.T) {
	src := []sample{{UserID: "u1"}, {UserID: "u2"}}
	v, err := DecodeCached[[]sample](src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(v) != 2 || v[0].UserID != "u1" || v[1].UserID != "u2" {
		t.Fatalf("expected direct slice return, got %+v", v)
	}
}

// TestDecodeCached_SliceInterfaceMiddleType 缓存命中（L2 JSON 数组）：[]interface{} 经 JSON 中转还原。
func TestDecodeCached_SliceInterfaceMiddleType(t *testing.T) {
	raw := []interface{}{
		map[string]interface{}{"user_id": "u1"},
		map[string]interface{}{"user_id": "u2"},
	}
	v, err := DecodeCached[[]sample](raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(v) != 2 || v[0].UserID != "u1" || v[1].UserID != "u2" {
		t.Fatalf("expected json transit slice restore, got %+v", v)
	}
}

// TestDecodeCached_EmptySlice 空切片：JSON 中转后仍为空切片，不 panic。
func TestDecodeCached_EmptySlice(t *testing.T) {
	raw := []interface{}{}
	v, err := DecodeCached[[]sample](raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v == nil || len(v) != 0 {
		t.Fatalf("expected empty slice, got %+v", v)
	}
}
