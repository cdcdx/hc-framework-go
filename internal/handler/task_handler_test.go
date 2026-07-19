package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/repository"
)

func claimReq(r *ginEngine, taskID string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/"+taskID+"/claim", nil)
	r.ServeHTTP(w, req)
	return w
}

// TestTaskHandler_Unauthorized 验证未鉴权时各任务接口均返回 401。
func TestTaskHandler_Unauthorized(t *testing.T) {
	svc, _ := newTestTask(t)
	h := NewTaskHandler(svc)
	r := authedEngine("", func(r *ginEngine) {
		r.GET("/api/v1/tasks", h.List)
		r.GET("/api/v1/tasks/progress", h.Progress)
		r.POST("/api/v1/tasks/1/claim", h.Claim)
	})
	for _, path := range []string{"/api/v1/tasks", "/api/v1/tasks/progress", "/api/v1/tasks/1/claim"} {
		w := httptest.NewRecorder()
		method := http.MethodGet
		if path == "/api/v1/tasks/1/claim" {
			method = http.MethodPost
		}
		r.ServeHTTP(w, httptest.NewRequest(method, path, nil))
		if codeOf(t, w) != model.CodeTokenInvalid {
			t.Fatalf("path %s code = %v, want %d", path, codeOf(t, w), model.CodeTokenInvalid)
		}
	}
}

// TestTaskHandler_Claim_InvalidID 验证非法任务 ID 返回 400。
func TestTaskHandler_Claim_InvalidID(t *testing.T) {
	svc, _ := newTestTask(t)
	h := NewTaskHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.POST("/api/v1/tasks/:id/claim", h.Claim)
	})
	w := claimReq(r, "abc")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (invalid task id)", w.Code)
	}
}

// TestTaskHandler_Claim_TaskNotFound 验证任务不存在返回 10002。
func TestTaskHandler_Claim_TaskNotFound(t *testing.T) {
	svc, _ := newTestTask(t)
	h := NewTaskHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.POST("/api/v1/tasks/:id/claim", h.Claim)
	})
	w := claimReq(r, "999999")
	if codeOf(t, w) != model.CodeNotFound {
		t.Fatalf("code = %v, want %d (task not found)", codeOf(t, w), model.CodeNotFound)
	}
}

// TestTaskHandler_Claim_TaskNotCompleted 验证进度未完成返回 10501。
func TestTaskHandler_Claim_TaskNotCompleted(t *testing.T) {
	svc, gdb := newTestTask(t)
	seedUser(t, gdb, "u1", 0)
	task := &model.Task{TaskType: "daily", TaskKey: "test_notdone", TaskName: "t", TargetValue: 5, RewardPoints: 100, IsActive: true}
	gdb.Create(task)
	// 进度为 0，未完成
	period := repository.GetCurrentPeriod(task.TaskType)
	gdb.Create(&model.UserTaskProgress{UserID: "u1", TaskID: task.ID, Period: period, CurrentProgress: 0, IsCompleted: false})

	h := NewTaskHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.POST("/api/v1/tasks/:id/claim", h.Claim)
	})
	w := claimReq(r, fmt.Sprintf("%d", task.ID))
	if codeOf(t, w) != model.CodeTaskNotCompleted {
		t.Fatalf("code = %v, want %d (task not completed)", codeOf(t, w), model.CodeTaskNotCompleted)
	}
}

// TestTaskHandler_Claim_Success 验证进度完成后领取成功；重复领取返回 10502。
func TestTaskHandler_Claim_Success(t *testing.T) {
	svc, gdb := newTestTask(t)
	seedUser(t, gdb, "u1", 0)
	task := &model.Task{TaskType: "daily", TaskKey: "test_done", TaskName: "t", TargetValue: 5, RewardPoints: 100, IsActive: true}
	gdb.Create(task)
	period := repository.GetCurrentPeriod(task.TaskType)
	gdb.Create(&model.UserTaskProgress{UserID: "u1", TaskID: task.ID, Period: period, CurrentProgress: 5, IsCompleted: true, IsClaimed: false})

	h := NewTaskHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.POST("/api/v1/tasks/:id/claim", h.Claim)
	})
	w := claimReq(r, fmt.Sprintf("%d", task.ID))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}
	// 重复领取应返回 10502
	w2 := claimReq(r, fmt.Sprintf("%d", task.ID))
	if codeOf(t, w2) != model.CodeTaskClaimed {
		t.Fatalf("second claim code = %v, want %d (already claimed)", codeOf(t, w2), model.CodeTaskClaimed)
	}
}

// TestTaskHandler_List_Success 验证任务列表返回已 seed 的任务。
func TestTaskHandler_List_Success(t *testing.T) {
	svc, _ := newTestTask(t)
	h := NewTaskHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.GET("/api/v1/tasks", h.List)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/tasks", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	data := body["data"].([]interface{})
	if len(data) < 7 {
		t.Fatalf("tasks len = %d, want >= 7 (seeded)", len(data))
	}
}
