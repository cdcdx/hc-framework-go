package handler

import (
	"net/http"
	"sync/atomic"

	"github.com/cdcdx/hc-framework-go/internal/cache"
	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/pkg/logger"
	"github.com/cdcdx/hc-framework-go/pkg/response"
	"github.com/gin-gonic/gin"
)

// serving 标记本 Pod 是否处于「可接收流量」状态（就绪探针据此返回 200/503）。
// 启动完成后由 bootstrap 置 true；优雅关闭一开始即置 false，使 K8s 就绪探针失败、
// 负载均衡把本 Pod 从端点摘除，进入「先停新流量、再排空在途请求」的优雅排水流程
// （drain），避免关闭瞬间仍向本 Pod 转发请求导致 5xx（高可用优雅下线，见文档 20）。
// 使用进程级 atomic 标志，与具体 HealthHandler 实例解耦，优雅关闭时无需持有 handler 引用即可切换。
var serving atomic.Bool

// init 将就绪标志初始化为 true（atomic.Bool 零值为 false）。启动时 HTTP 尚未监听，探针无从
// 访问，不会误判；HTTP 一旦监听即视为就绪。优雅关闭由 bootstrap 置 false 触发排水。
func init() { serving.Store(true) }

// SetServing 设置就绪状态（true=就绪可接收流量，false=正在关闭、停止接收新流量）。
// 由 bootstrap 在 HTTP 启动后、优雅关闭开始时分别调用。
func SetServing(s bool) { serving.Store(s) }

// HealthHandler 健康检查处理器
type HealthHandler struct {
	cfg      *config.Config
	cfgMgr   *config.Manager // 可选：配置管理器（用于 /debug/reload 热更新）
	cacheMgr *cache.Manager  // 可选：用于暴露缓存健康/统计（可为 nil）
}

// NewHealthHandler 创建健康检查处理器
// cacheMgr 可为 nil（未启用缓存时）；cfgMgr 可为 nil（未启用配置热更新时）
func NewHealthHandler(cfg *config.Config, cacheMgr *cache.Manager, cfgMgr *config.Manager) *HealthHandler {
	return &HealthHandler{cfg: cfg, cacheMgr: cacheMgr, cfgMgr: cfgMgr}
}

// Health 健康检查
// @Summary      健康检查
// @Description  返回服务健康状态，包含各组件检查结果
// @Tags         系统
// @Produce      json
// @Success      200  {object}  map[string]interface{}
// @Router       /health [get]
func (h *HealthHandler) Health(c *gin.Context) {
	checks := map[string]interface{}{
		"server": "ok",
	}
	status := "ok"

	if h.cacheMgr != nil {
		cacheHealth := h.cacheMgr.Health(c.Request.Context())
		checks["cache"] = cacheHealth
		if l2ok, ok := cacheHealth["l2_ok"].(bool); ok && !l2ok {
			status = "degraded"
		}
	}

	health := map[string]interface{}{
		"status":  status,
		"checks":  checks,
		"version": "1.0.0",
	}

	c.JSON(http.StatusOK, health)
}

// Ready 就绪检查
// @Summary      就绪检查
// @Description  检查服务是否已准备好接收流量
// @Tags         系统
// @Produce      json
// @Success      200  {object}  map[string]interface{}
// @Failure      503  {object}  map[string]interface{}
// @Router       /ready [get]
func (h *HealthHandler) Ready(c *gin.Context) {
	// 优雅关闭进行中：返回 503，K8s 就绪探针失败 → 本 Pod 从 Service 端点摘除，停止接收新流量。
	// 配合 bootstrap.shutdown 首步 SetServing(false) 实现「先摘流量、再排空在途请求」的优雅下线。
	if !serving.Load() {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "shutting_down",
			"message": "service is draining, not accepting new traffic",
		})
		return
	}
	ready := map[string]interface{}{
		"status":  "ok",
		"message": "service is ready to accept traffic",
	}

	c.JSON(http.StatusOK, ready)
}

// CacheMetrics 缓存统计端点（JSON，非 Prometheus 格式；Prometheus 指标见 /metrics）
// @Summary      缓存统计
// @Description  暴露缓存组件（L1/L2/布隆/热点Key）的运行时统计（JSON）
// @Tags         系统
// @Produce      json
// @Success      200  {object}  map[string]interface{}
// @Router       /metrics/cache [get]
func (h *HealthHandler) CacheMetrics(c *gin.Context) {
	if h.cacheMgr == nil {
		response.Success(c, gin.H{
			"message": "cache disabled, no metrics available",
			"version": "1.0.0",
		})
		return
	}
	stats := h.cacheMgr.Stats()
	stats["version"] = "1.0.0"
	response.Success(c, stats)
}

// SetLogLevel 动态调整日志级别
// @Summary      动态调整日志级别
// @Description  运行时动态修改日志级别
// @Tags         系统
// @Accept       json
// @Produce      json
// @Param        body  body      object{level=string}  true  "日志级别"
// @Success      200   {object}  map[string]interface{}
// @Router       /debug/loglevel [put]
func (h *HealthHandler) SetLogLevel(c *gin.Context) {
	var req struct {
		Level string `json:"level" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "level is required")
		return
	}

	// 动态调整 Zap 日志级别
	if err := logger.SetLevel(req.Level); err != nil {
		response.BadRequest(c, "invalid log level, valid: debug, info, warn, error, fatal")
		return
	}

	response.Success(c, gin.H{
		"level":   req.Level,
		"message": "log level updated",
	})
}

// Reload 通过 HTTP API 触发配置热更新（需求 §9）
// @Summary      配置热更新
// @Description  重新读取配置文件并原子替换生效配置（限流/熔断/缓存降级等参数立即生效）
// @Tags         系统
// @Produce      json
// @Success      200  {object}  map[string]interface{}
// @Failure      500  {object}  map[string]interface{}
// @Router       /debug/reload [post]
func (h *HealthHandler) Reload(c *gin.Context) {
	if h.cfgMgr == nil {
		response.Error(c, model.CodeUnknownError, "config manager not available")
		return
	}
	if err := h.cfgMgr.Reload(); err != nil {
		response.Error(c, model.CodeUnknownError, err.Error())
		return
	}
	response.Success(c, gin.H{
		"message": "config reloaded",
		"version": h.cfgMgr.Version(),
	})
}
