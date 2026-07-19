package handler

import (
	"github.com/cdcdx/hc-framework-go/internal/service/idle"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/pkg/response"
)

// IdleHandler 挂机处理器
type IdleHandler struct {
	svc *idle.IdleService
}

// NewIdleHandler 创建挂机处理器
func NewIdleHandler(svc *idle.IdleService) *IdleHandler {
	return &IdleHandler{svc: svc}
}

// Start 开始挂机
// @Summary      开始挂机
// @Description  启动挂机计时（支持多设备同时挂机），开始累计积分
// @Tags         挂机
// @Security     BearerAuth
// @Accept       json
// @Produce      json
// @Param        body  body  object{device_id=string}  true  "设备ID"
// @Success      200   {object}  map[string]interface{}
// @Failure      400   {object}  map[string]interface{}  "设备数量超限或参数错误"
// @Failure      401   {object}  map[string]interface{}
// @Router       /api/v1/idle/start [post]
func (h *IdleHandler) Start(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	var req struct {
		DeviceID string `json:"device_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "device_id is required")
		return
	}

	record, err := h.svc.Start(c.Request.Context(), userID, req.DeviceID)
	if err != nil {
		if err == idle.ErrMaxDevices {
			response.Error(c, model.CodeUnknownError, "maximum active devices reached")
			return
		}
		response.Error(c, model.CodeUnknownError, err.Error())
		return
	}

	response.Success(c, record)
}

// Heartbeat 心跳上报
// @Summary      心跳上报
// @Description  挂机期间定期上报心跳，需传入 device_id 精确定位会话
// @Tags         挂机
// @Security     BearerAuth
// @Accept       json
// @Produce      json
// @Param        body  body  object{device_id=string}  true  "设备ID"
// @Success      200   {object}  map[string]interface{}
// @Failure      400   {object}  map[string]interface{}  "未在挂机状态"
// @Failure      401   {object}  map[string]interface{}
// @Router       /api/v1/idle/heartbeat [post]
func (h *IdleHandler) Heartbeat(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	var req struct {
		DeviceID string `json:"device_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "device_id is required")
		return
	}

	if err := h.svc.Heartbeat(c.Request.Context(), userID, req.DeviceID); err != nil {
		if err == idle.ErrNotIdle {
			response.Error(c, model.CodeNotIdle)
			return
		}
		response.Error(c, model.CodeUnknownError, err.Error())
		return
	}

	response.Success(c, gin.H{"status": "alive"})
}

// Stop 停止全部设备挂机
// @Summary      停止挂机（全部设备）
// @Description  停止所有设备的挂机计时，结算当前积分
// @Tags         挂机
// @Security     BearerAuth
// @Produce      json
// @Success      200  {object}  map[string]interface{}
// @Failure      400  {object}  map[string]interface{}  "未在挂机状态"
// @Failure      401  {object}  map[string]interface{}
// @Router       /api/v1/idle/stop [post]
func (h *IdleHandler) Stop(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	record, err := h.svc.Stop(c.Request.Context(), userID)
	if err != nil {
		if err == idle.ErrNotIdle {
			response.Error(c, model.CodeNotIdle)
			return
		}
		response.Error(c, model.CodeUnknownError, err.Error())
		return
	}

	response.Success(c, record)
}

// StopDevice 停止指定设备挂机
// @Summary      停止指定设备挂机
// @Description  停止指定设备的挂机计时，结算积分
// @Tags         挂机
// @Security     BearerAuth
// @Accept       json
// @Produce      json
// @Param        body  body  object{device_id=string}  true  "设备ID"
// @Success      200   {object}  map[string]interface{}
// @Failure      400   {object}  map[string]interface{}  "未在挂机状态"
// @Failure      401   {object}  map[string]interface{}
// @Router       /api/v1/idle/stop-device [post]
func (h *IdleHandler) StopDevice(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	var req struct {
		DeviceID string `json:"device_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "device_id is required")
		return
	}

	record, err := h.svc.StopDevice(c.Request.Context(), userID, req.DeviceID)
	if err != nil {
		if err == idle.ErrNotIdle {
			response.Error(c, model.CodeNotIdle)
			return
		}
		response.Error(c, model.CodeUnknownError, err.Error())
		return
	}

	response.Success(c, record)
}

// Status 查询挂机状态
// @Summary      查询挂机状态
// @Description  查询当前用户所有设备的挂机状态（支持多设备）
// @Tags         挂机
// @Security     BearerAuth
// @Produce      json
// @Success      200  {object}  map[string]interface{}
// @Failure      401  {object}  map[string]interface{}
// @Router       /api/v1/idle/status [get]
func (h *IdleHandler) Status(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	status, err := h.svc.Status(c.Request.Context(), userID)
	if err != nil {
		response.Error(c, model.CodeUnknownError, err.Error())
		return
	}

	response.Success(c, status)
}

// Records 查询挂机历史记录
// @Summary      查询挂机记录
// @Description  分页查询用户的挂机历史记录（游标分页）
// @Tags         挂机
// @Security     BearerAuth
// @Produce      json
// @Param        cursor   query     string  false  "游标"
// @Param        limit    query     int     false  "每页条数"
// @Success      200      {object}  map[string]interface{}
// @Failure      401      {object}  map[string]interface{}
// @Router       /api/v1/idle/records [get]
func (h *IdleHandler) Records(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	cursor, err := strconv.ParseInt(c.Query("cursor"), 10, 64)
	if c.Query("cursor") != "" && err != nil {
		response.BadRequest(c, "invalid cursor")
		return
	}
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if err != nil {
		limit = 20
	}

	records, nextCursor, hasMore, err := h.svc.Records(c.Request.Context(), userID, cursor, limit)
	if err != nil {
		response.Error(c, model.CodeDBError, err.Error())
		return
	}

	response.SuccessPage(c, records, strconv.FormatInt(nextCursor, 10), hasMore)
}
