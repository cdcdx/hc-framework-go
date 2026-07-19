package handler

import (
	tasksvc "github.com/cdcdx/hc-framework-go/internal/service/task"
	"strconv"

	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/pkg/response"
	"github.com/gin-gonic/gin"
)

// TaskHandler 任务处理器
type TaskHandler struct {
	svc *tasksvc.TaskService
}

// NewTaskHandler 创建任务处理器
func NewTaskHandler(svc *tasksvc.TaskService) *TaskHandler {
	return &TaskHandler{svc: svc}
}

// List 获取任务列表
// @Summary      任务列表
// @Description  获取所有可用任务列表（含用户进度）
// @Tags         任务
// @Security     BearerAuth
// @Produce      json
// @Success      200  {object}  map[string]interface{}
// @Failure      401  {object}  map[string]interface{}
// @Router       /api/v1/tasks [get]
func (h *TaskHandler) List(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	tasks, err := h.svc.List(c.Request.Context(), userID)
	if err != nil {
		response.Error(c, model.CodeDBError, err.Error())
		return
	}

	response.Success(c, tasks)
}

// Progress 查询任务进度
// @Summary      任务进度
// @Description  查询当前用户的任务完成进度
// @Tags         任务
// @Security     BearerAuth
// @Produce      json
// @Success      200  {object}  map[string]interface{}
// @Failure      401  {object}  map[string]interface{}
// @Router       /api/v1/tasks/progress [get]
func (h *TaskHandler) Progress(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	progress, err := h.svc.Progress(c.Request.Context(), userID)
	if err != nil {
		response.Error(c, model.CodeDBError, err.Error())
		return
	}

	response.Success(c, progress)
}

// Claim 领取任务奖励
// @Summary      领取任务奖励
// @Description  领取已完成任务的奖励
// @Tags         任务
// @Security     BearerAuth
// @Produce      json
// @Param        id   path      int  true  "任务ID"
// @Success      200  {object}  map[string]interface{}
// @Failure      401  {object}  map[string]interface{}
// @Router       /api/v1/tasks/{id}/claim [post]
func (h *TaskHandler) Claim(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	taskID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.BadRequest(c, "invalid task id")
		return
	}

	task, err := h.svc.Claim(c.Request.Context(), userID, taskID)
	if err != nil {
		switch err {
		case tasksvc.ErrTaskNotFound:
			response.Error(c, model.CodeNotFound, "task not found")
		case tasksvc.ErrTaskNotCompleted:
			response.Error(c, model.CodeTaskNotCompleted)
		case tasksvc.ErrTaskClaimed:
			response.Error(c, model.CodeTaskClaimed)
		default:
			response.Error(c, model.CodeUnknownError, err.Error())
		}
		return
	}

	response.Success(c, gin.H{
		"task":          task,
		"reward_points": task.RewardPoints,
	})
}
