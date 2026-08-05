package response

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cdcdx/hc-framework-go/common/errorx"
)

func decode(t *testing.T, body *strings.Reader) Response {
	t.Helper()
	var resp Response
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
}

func TestSuccess(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	Success(w, r, map[string]any{"k": "v"})

	if w.Code != http.StatusOK {
		t.Fatalf("http status = %d, want 200", w.Code)
	}
	resp := decode(t, strings.NewReader(w.Body.String()))
	if resp.Code != errorx.CodeSuccess {
		t.Fatalf("code = %d, want %d", resp.Code, errorx.CodeSuccess)
	}
	if resp.Message != "success" {
		t.Fatalf("message = %q, want success", resp.Message)
	}
	data, ok := resp.Data.(map[string]any)
	if !ok || data["k"] != "v" {
		t.Fatalf("data = %#v, want map with k=v", resp.Data)
	}
}

func TestSuccessPage(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	SuccessPage(w, r, []any{"a"}, "next", true)

	resp := decode(t, strings.NewReader(w.Body.String()))
	page, ok := resp.Data.(map[string]any)
	if !ok {
		t.Fatalf("data = %#v, want PageResponse", resp.Data)
	}
	if page["next_cursor"] != "next" || page["has_more"] != true {
		t.Fatalf("page = %#v", page)
	}
}

func TestError_UseCustomMsg(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	Error(w, r, errorx.CodeNotFound, "自定义消息")

	resp := decode(t, strings.NewReader(w.Body.String()))
	if resp.Code != errorx.CodeNotFound {
		t.Fatalf("code = %d, want %d", resp.Code, errorx.CodeNotFound)
	}
	if resp.Message != "自定义消息" {
		t.Fatalf("message = %q, want 自定义消息", resp.Message)
	}
}

func TestError_FallbackToCodeMsg(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	Error(w, r, errorx.CodeNotFound)

	resp := decode(t, strings.NewReader(w.Body.String()))
	if resp.Message != "资源不存在" {
		t.Fatalf("message = %q, want 资源不存在", resp.Message)
	}
}

func TestBadRequest_HTTPStatus(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	BadRequest(w, r, "bad param")

	if w.Code != http.StatusBadRequest {
		t.Fatalf("http status = %d, want 400", w.Code)
	}
	resp := decode(t, strings.NewReader(w.Body.String()))
	if resp.Code != errorx.CodeInvalidParam {
		t.Fatalf("code = %d, want %d", resp.Code, errorx.CodeInvalidParam)
	}
}

func TestRateLimited_RetryAfterHeader(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	RateLimited(w, r, 42)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("http status = %d, want 429", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got != "42" {
		t.Fatalf("Retry-After = %q, want 42", got)
	}
}

func TestUnauthorized_Forbidden(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	Unauthorized(w, r, errorx.CodeTokenExpired)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("http status = %d, want 401", w.Code)
	}

	w2 := httptest.NewRecorder()
	Forbidden(w2, r, errorx.CodePermissionDenied)
	if w2.Code != http.StatusForbidden {
		t.Fatalf("http status = %d, want 403", w2.Code)
	}
}
