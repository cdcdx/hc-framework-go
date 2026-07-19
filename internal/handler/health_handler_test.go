package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/pkg/logger"
	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func newHealthServer() *gin.Engine {
	r := gin.New()
	h := NewHealthHandler(&config.Config{}, nil, nil)
	r.GET("/health", h.Health)
	r.GET("/ready", h.Ready)
	r.GET("/metrics/cache", h.CacheMetrics)
	r.PUT("/debug/loglevel", h.SetLogLevel)
	r.POST("/debug/reload", h.Reload)
	return r
}

func decodeBody(t *testing.T, w *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body %q: %v", w.Body.String(), err)
	}
	return body
}

func TestHealthHandler_Health(t *testing.T) {
	srv := newHealthServer()
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := decodeBody(t, w)
	if body["status"] != "ok" {
		t.Fatalf("status = %v, want ok", body["status"])
	}
	if body["version"] != "1.0.0" {
		t.Fatalf("version = %v, want 1.0.0", body["version"])
	}
}

func TestHealthHandler_Ready(t *testing.T) {
	srv := newHealthServer()
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := decodeBody(t, w)
	if body["status"] != "ok" {
		t.Fatalf("ready status = %v, want ok", body["status"])
	}
}

func TestHealthHandler_CacheMetricsNilCache(t *testing.T) {
	srv := newHealthServer()
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics/cache", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := decodeBody(t, w)
	data, ok := body["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("data missing or wrong type: %v", body["data"])
	}
	if _, ok := data["message"]; !ok {
		t.Fatalf("expected cache-disabled message, got %v", data)
	}
}

func TestHealthHandler_SetLogLevel(t *testing.T) {
	srv := newHealthServer()
	defer func() {
		// 还原全局日志级别，避免影响其它测试
		_ = logger.SetLevel("info")
	}()

	// 合法级别
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/debug/loglevel",
		strings.NewReader(`{"level":"debug"}`)))
	if w.Code != http.StatusOK {
		t.Fatalf("valid level status = %d, want 200", w.Code)
	}

	// 非法级别
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/debug/loglevel",
		strings.NewReader(`{"level":"verbose"}`)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid level status = %d, want 400", w.Code)
	}
	body := decodeBody(t, w)
	if int(body["code"].(float64)) != model.CodeInvalidParam {
		t.Fatalf("code = %v, want %d", body["code"], model.CodeInvalidParam)
	}
}

func TestHealthHandler_ReloadNilManager(t *testing.T) {
	srv := newHealthServer()
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/debug/reload", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := decodeBody(t, w)
	if int(body["code"].(float64)) != model.CodeUnknownError {
		t.Fatalf("code = %v, want %d (config manager unavailable)", body["code"], model.CodeUnknownError)
	}
}

// TestHealthHandler_ReadyDrain 验证优雅下线：关闭期服务（serving=false）时 /ready 返回 503，
// 使 K8s 就绪探针失败、本 Pod 从 Service 端点摘除，进入「先停新流量、再排空在途请求」的排水流程。
func TestHealthHandler_ReadyDrain(t *testing.T) {
	defer SetServing(true) // 还原全局就绪标志，避免影响其它测试

	SetServing(false)
	srv := newHealthServer()
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("draining ready status = %d, want 503", w.Code)
	}
	body := decodeBody(t, w)
	if body["status"] != "shutting_down" {
		t.Fatalf("draining status = %v, want shutting_down", body["status"])
	}

	// 恢复就绪后 /ready 重新返回 200
	SetServing(true)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("recovered ready status = %d, want 200", w.Code)
	}
}
