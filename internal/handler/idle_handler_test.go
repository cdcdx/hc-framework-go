package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/model"
)

// TestIdleHandler_Unauthorized 验证未鉴权时所有挂机接口均返回 401。
func TestIdleHandler_Unauthorized(t *testing.T) {
	svc, _ := newTestIdle(t)
	h := NewIdleHandler(svc)
	r := authedEngine("", func(r *ginEngine) {
		r.POST("/api/v1/idle/start", h.Start)
		r.POST("/api/v1/idle/heartbeat", h.Heartbeat)
		r.POST("/api/v1/idle/stop", h.Stop)
		r.POST("/api/v1/idle/stop-device", h.StopDevice)
		r.GET("/api/v1/idle/status", h.Status)
		r.GET("/api/v1/idle/records", h.Records)
	})
	cases := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/api/v1/idle/start"},
		{http.MethodPost, "/api/v1/idle/heartbeat"},
		{http.MethodPost, "/api/v1/idle/stop"},
		{http.MethodPost, "/api/v1/idle/stop-device"},
		{http.MethodGet, "/api/v1/idle/status"},
		{http.MethodGet, "/api/v1/idle/records"},
	}
	for _, c := range cases {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(c.method, c.path, nil))
		if codeOf(t, w) != model.CodeTokenInvalid {
			t.Fatalf("%s %s code = %v, want %d", c.method, c.path, codeOf(t, w), model.CodeTokenInvalid)
		}
	}
}

// TestIdleHandler_Start_MissingDevice 验证缺少 device_id 返回 400。
func TestIdleHandler_Start_MissingDevice(t *testing.T) {
	svc, _ := newTestIdle(t)
	h := NewIdleHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.POST("/api/v1/idle/start", h.Start)
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/idle/start", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (missing device_id)", w.Code)
	}
}

// TestIdleHandler_Heartbeat_MissingDevice 验证缺少 device_id 返回 400。
func TestIdleHandler_Heartbeat_MissingDevice(t *testing.T) {
	svc, _ := newTestIdle(t)
	h := NewIdleHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.POST("/api/v1/idle/heartbeat", h.Heartbeat)
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/idle/heartbeat", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (missing device_id)", w.Code)
	}
}

// TestIdleHandler_StopDevice_MissingDevice 验证缺少 device_id 返回 400。
func TestIdleHandler_StopDevice_MissingDevice(t *testing.T) {
	svc, _ := newTestIdle(t)
	h := NewIdleHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.POST("/api/v1/idle/stop-device", h.StopDevice)
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/idle/stop-device", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (missing device_id)", w.Code)
	}
}

// TestIdleHandler_Records_InvalidCursor 验证非法游标返回 400。
func TestIdleHandler_Records_InvalidCursor(t *testing.T) {
	svc, _ := newTestIdle(t)
	h := NewIdleHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.GET("/api/v1/idle/records", h.Records)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/idle/records?cursor=abc", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (invalid cursor)", w.Code)
	}
}

// TestIdleHandler_Stop_NotIdle 验证无活跃会话时停止返回 10402。
func TestIdleHandler_Stop_NotIdle(t *testing.T) {
	svc, _ := newTestIdle(t)
	h := NewIdleHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.POST("/api/v1/idle/stop", h.Stop)
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/idle/stop", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if codeOf(t, w) != model.CodeNotIdle {
		t.Fatalf("code = %v, want %d (not idle)", codeOf(t, w), model.CodeNotIdle)
	}
}

// TestIdleHandler_StopDevice_NotIdle 验证无该设备活跃会话时返回 10402。
func TestIdleHandler_StopDevice_NotIdle(t *testing.T) {
	svc, _ := newTestIdle(t)
	h := NewIdleHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.POST("/api/v1/idle/stop-device", h.StopDevice)
	})
	body, _ := json.Marshal(map[string]interface{}{"device_id": "dX"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/idle/stop-device", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if codeOf(t, w) != model.CodeNotIdle {
		t.Fatalf("code = %v, want %d (not idle)", codeOf(t, w), model.CodeNotIdle)
	}
}

// TestIdleHandler_Status_NotIdle 验证无活跃会话时 is_idle 为 false。
func TestIdleHandler_Status_NotIdle(t *testing.T) {
	svc, _ := newTestIdle(t)
	h := NewIdleHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.GET("/api/v1/idle/status", h.Status)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/idle/status", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var body map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	data := body["data"].(map[string]interface{})
	if data["is_idle"] != false {
		t.Fatalf("is_idle = %v, want false", data["is_idle"])
	}
}

// TestIdleHandler_Records_Success 验证历史记录查询返回已落库记录。
func TestIdleHandler_Records_Success(t *testing.T) {
	svc, gdb := newTestIdle(t)
	now := time.Now()
	for i := 0; i < 3; i++ {
		gdb.Create(&model.IdleRecord{
			UserID:       "u1",
			DeviceID:     "d" + string(rune('1'+i)),
			StartTime:    now.Add(-time.Duration(i+1) * time.Hour),
			Status:       "completed",
			PointsEarned: int64(10 * (i + 1)),
		})
	}
	h := NewIdleHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.GET("/api/v1/idle/records", h.Records)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/idle/records", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}
	var body map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	data := body["data"].(map[string]interface{})
	items := data["items"].([]interface{})
	if len(items) != 3 {
		t.Fatalf("records len = %d, want 3", len(items))
	}
}
