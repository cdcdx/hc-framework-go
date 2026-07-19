package repository

import (
	"strings"
	"testing"
)

// TestAggregateBulkErrors 覆盖 ES bulk 响应的逐条失败聚合逻辑（13 §3.42 ①），
// 无需真实 ES：直接喂入 bulk 响应的 JSON 体，验证三种情形。
func TestAggregateBulkErrors(t *testing.T) {
	// 1) 全成功：errors=false，应返回 nil（不再被 res.IsError() 误吞为成功）
	ok := `{"errors":false,"items":[{"index":{"status":201}}]}`
	if err := aggregateBulkErrors(strings.NewReader(ok)); err != nil {
		t.Fatalf("all-success bulk: got %v, want nil", err)
	}

	// 2) 部分失败：errors=true 且存在逐条 error，应聚合并返回 error
	partial := `{"errors":true,"items":[` +
		`{"index":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse"}}},` +
		`{"index":{"status":201}}]}`
	err := aggregateBulkErrors(strings.NewReader(partial))
	if err == nil {
		t.Fatal("partial-failure bulk: want aggregated error, got nil")
	}
	if !strings.Contains(err.Error(), "partial failures (1/2)") {
		t.Fatalf("partial-failure message unexpected: %v", err)
	}

	// 3) 非法 JSON：应返回 decode 错误
	if err := aggregateBulkErrors(strings.NewReader("not-json")); err == nil {
		t.Fatal("invalid json: want decode error, got nil")
	}
}
