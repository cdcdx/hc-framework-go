package response

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// newTestCtx 构造一个用于测试响应的 gin.Context，返回 ctx 与捕获的响应记录器。
func newTestCtx() (*gin.Context, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	return c, w
}

// decode 将响应体解析为通用 map 便于断言。
func decode(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return m
}

func TestSuccess(t *testing.T) {
	c, w := newTestCtx()
	Success(c, map[string]int{"k": 1})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	m := decode(t, w.Body.Bytes())
	if int(m["code"].(float64)) != 0 {
		t.Errorf("code = %v, want 0", m["code"])
	}
	if m["data"] == nil {
		t.Error("data should not be nil")
	}
}

func TestSuccessPage(t *testing.T) {
	c, w := newTestCtx()
	SuccessPage(c, []int{1, 2}, "cursor-99", true)

	m := decode(t, w.Body.Bytes())
	data := m["data"].(map[string]any)
	if data["next_cursor"] != "cursor-99" {
		t.Errorf("next_cursor = %v, want cursor-99", data["next_cursor"])
	}
	if data["has_more"] != true {
		t.Errorf("has_more = %v, want true", data["has_more"])
	}
}

func TestError_WithCustomMsg(t *testing.T) {
	c, w := newTestCtx()
	Error(c, 10001, "自定义错误")
	m := decode(t, w.Body.Bytes())
	if int(m["code"].(float64)) != 10001 {
		t.Fatalf("code = %v, want 10001", m["code"])
	}
	if m["message"] != "自定义错误" {
		t.Errorf("message = %v, want 自定义错误", m["message"])
	}
}

func TestError_DefaultMsg(t *testing.T) {
	c, w := newTestCtx()
	Error(c, 10002)
	m := decode(t, w.Body.Bytes())
	// 默认消息来自 model.Message(10002)，不应为空
	if m["message"] == "" {
		t.Error("message should fall back to model.Message")
	}
}

func TestErrorWithHTTPStatus(t *testing.T) {
	c, w := newTestCtx()
	ErrorWithHTTPStatus(c, http.StatusTooManyRequests, 10402)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", w.Code)
	}
	m := decode(t, w.Body.Bytes())
	if int(m["code"].(float64)) != 10402 {
		t.Errorf("code = %v, want 10402", m["code"])
	}
}

func TestRateLimited(t *testing.T) {
	c, w := newTestCtx()
	RateLimited(c, 30)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got != "30" {
		t.Errorf("Retry-After = %q, want 30", got)
	}
}

func TestBadRequest(t *testing.T) {
	c, w := newTestCtx()
	BadRequest(c, "参数非法")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestUnauthorized(t *testing.T) {
	c, w := newTestCtx()
	Unauthorized(c, 10102)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestForbidden(t *testing.T) {
	c, w := newTestCtx()
	Forbidden(c, 10103)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", w.Code)
	}
}

func TestServiceUnavailable(t *testing.T) {
	c, w := newTestCtx()
	ServiceUnavailable(c, 10701)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
}

func TestGetTraceID(t *testing.T) {
	c, _ := newTestCtx()
	if got := getTraceID(c); got != "" {
		t.Errorf("empty ctx trace_id = %q, want empty", got)
	}
	c.Set("trace_id", "trace-abc")
	if got := getTraceID(c); got != "trace-abc" {
		t.Errorf("trace_id = %q, want trace-abc", got)
	}
	// 非 string 类型应安全回退为空
	c.Set("trace_id", 123)
	if got := getTraceID(c); got != "" {
		t.Errorf("non-string trace_id = %q, want empty", got)
	}
}

func TestResolveMsg(t *testing.T) {
	if got := resolveMsg(10001, "优先"); got != "优先" {
		t.Errorf("resolveMsg custom = %q, want 优先", got)
	}
	if got := resolveMsg(10001); got == "" {
		t.Error("resolveMsg fallback should not be empty")
	}
}
