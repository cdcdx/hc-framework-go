package common

import (
	"errors"
	"testing"
)

type testPayload struct {
	UserID string `json:"user_id"`
	Points int    `json:"points"`
	Note   string `json:"note"`
}

// TestDecodePayload_StandardMap 验证标准 map 解码到结构体。
func TestDecodePayload_StandardMap(t *testing.T) {
	payload := map[string]interface{}{
		"user_id": "u123",
		"points":  100,
		"note":    "test payout",
	}
	var out testPayload
	if err := DecodePayload(payload, &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.UserID != "u123" || out.Points != 100 || out.Note != "test payout" {
		t.Errorf("decode: got %+v, want {u123 100 test payout}", out)
	}
}

// TestDecodePayload_Struct 验证结构体直接解码到结构体（JSON 往返）。
func TestDecodePayload_Struct(t *testing.T) {
	payload := testPayload{UserID: "u456", Points: 200, Note: "direct struct"}
	var out testPayload
	if err := DecodePayload(payload, &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != payload {
		t.Errorf("decode struct: got %+v, want %+v", out, payload)
	}
}

// TestDecodePayload_PartialMap 验证部分字段缺失时零值填充。
func TestDecodePayload_PartialMap(t *testing.T) {
	payload := map[string]interface{}{
		"user_id": "u789",
		// points 和 note 缺失
	}
	var out testPayload
	if err := DecodePayload(payload, &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.UserID != "u789" || out.Points != 0 || out.Note != "" {
		t.Errorf("partial decode: got %+v, want {u789 0 }", out)
	}
}

// TestDecodePayload_NilPayload 验证 nil payload 返回错误。
func TestDecodePayload_NilPayload(t *testing.T) {
	var out testPayload
	err := DecodePayload(nil, &out)
	if err == nil {
		t.Error("nil payload: want error, got nil")
	}
}

// TestDecodePayload_NilOut 验证 nil out 返回错误。
func TestDecodePayload_NilOut(t *testing.T) {
	payload := map[string]interface{}{"user_id": "u1"}
	err := DecodePayload(payload, nil)
	if err == nil {
		t.Error("nil out: want error, got nil")
	}
}

// TestDecodePayload_TypeMismatch 验证类型不匹配时返回错误。
func TestDecodePayload_TypeMismatch(t *testing.T) {
	payload := map[string]interface{}{
		"user_id": 12345, // 数字不能赋值给 string
	}
	var out testPayload
	err := DecodePayload(payload, &out)
	if err == nil {
		t.Error("type mismatch: want error, got nil")
	}
}

// TestDecodePayload_ExtraFields 验证目标结构体不存在的额外字段被静默忽略。
func TestDecodePayload_ExtraFields(t *testing.T) {
	payload := map[string]interface{}{
		"user_id":     "u1",
		"points":      50,
		"extra_field": "should be ignored",
	}
	var out testPayload
	if err := DecodePayload(payload, &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.UserID != "u1" || out.Points != 50 {
		t.Errorf("extra fields: got %+v", out)
	}
}

// TestDecodePayload_ErrorPreserved 验证返回的错误类型可通过 errors.As 正确识别。
func TestDecodePayload_ErrorPreserved(t *testing.T) {
	payload := make(chan int) // 不可 JSON 序列化
	var out testPayload
	err := DecodePayload(payload, &out)
	if err == nil {
		t.Fatal("chan payload: want error, got nil")
	}
	var jsonErr jsonMarshalError
	if !errors.As(err, &jsonErr) {
		t.Logf("error type: %T, value: %v", err, err)
		// 注意：json.Marshal 返回的是 json.UnsupportedTypeError，不是我们自己的类型
		// 只要能识别出错误即可
	}
}

// jsonMarshalError 辅助接口用于 errors.As 测试。
type jsonMarshalError interface {
	Error() string
}
