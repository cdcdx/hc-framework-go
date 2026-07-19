// Package router HTTP 路由装配（中间件 + 处理器注册）。
package router

import (
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	"github.com/cdcdx/hc-framework-go/internal/handler"
	"github.com/cdcdx/hc-framework-go/internal/metrics"
	"github.com/cdcdx/hc-framework-go/internal/middleware"
)

// Setup 初始化路由
func Setup(deps *handler.Dependencies) *gin.Engine {
	cfg := deps.Cfg

	// 设置 Gin 模式
	gin.SetMode(cfg.Server.Mode)

	r := gin.New()

	// ==========================================
	// 全局中间件链（按顺序）
	// ==========================================
	r.Use(middleware.Recovery())
	// 请求超时：给所有请求的 ctx 上 deadline，使下游 DB/锁等待有界（13 §3.48）。
	// 置于 Recovery 之后、其余中间件之前：同 goroutine 执行 c.Next()，Recovery 仍能在
	// 同一栈捕获 panic；同时所有后续 handler 继承带 deadline 的 ctx。
	r.Use(middleware.Timeout(cfg))
	r.Use(middleware.Trace())
	r.Use(middleware.RequestInfo())
	r.Use(middleware.RequestLogger(cfg))
	r.Use(middleware.CORS(cfg))
	r.Use(middleware.Metrics())
	// 应用层有界并发 / 负载卸载（高并发高可用加固，见文档 20）：在连接级上限与令牌桶限流之后、
	// 业务处理器之前，对同时在途的请求数设硬上限，达到上限即快速失败 503，避免无界 goroutine
	// 在 DB/下游抖动时堆积压垮后端。系统/运维端点（/health、/ready、/metrics 等）已豁免，
	// 不影响 K8s 探针与指标抓取。容量由 server.concurrency_limit 配置，重启生效。
	r.Use(middleware.ConcurrencyLimit(deps.CfgMgr))

	// ==========================================
	// 系统接口（公开）
	// ==========================================
	healthHandler := handler.NewHealthHandler(cfg, deps.CacheMgr, deps.CfgMgr)
	r.GET("/health", healthHandler.Health)
	r.GET("/ready", healthHandler.Ready)
	// 原 JSON 缓存统计保留兼容（非 Prometheus 格式）；Prometheus 指标见 /metrics
	r.GET("/metrics/cache", healthHandler.CacheMetrics)
	if cfg.Metrics.Enabled {
		metrics.Init(deps.CacheMgr)
		metricsPath := cfg.Metrics.Path
		if metricsPath == "" {
			metricsPath = "/metrics"
		}
		r.GET(metricsPath, gin.WrapH(metrics.Handler()))
	}
	r.PUT("/debug/loglevel", healthHandler.SetLogLevel)
	// 配置热更新 HTTP API（需求 §9）：POST /debug/reload 重新加载配置文件（限流/熔断/缓存降级即时生效）
	r.POST("/debug/reload", healthHandler.Reload)

	// Swagger 文档
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// ==========================================
	// API v1
	// ==========================================
	v1 := r.Group("/api/v1")

	// --- 限流中间件（限流参数支持热更新） ---
	v1.Use(middleware.RateLimit(deps.CfgMgr))

	// --- 验证码防水墙中间件（作用于注册/登录等敏感接口） ---
	captchaMW := middleware.Captcha(cfg)

	// --- 认证模块（公开） ---
	authHandler := handler.NewAuthHandler(deps.AuthSvc)
	auth := v1.Group("/auth")
	// 公开认证路由同样挂熔断：register/login 直接写库，下游 DB/缓存抖动时快速失败并降级，
	// 避免连接耗尽在认证入口堆积（需求 §6.4）。熔断按路由独立、仅 ≥500 计入失败，4xx 不误触发
	// （与 authorized 组共用一个配置化熔断器管理器，支持 /debug/reload 热更新阈值）。
	auth.Use(middleware.CircuitBreaker(deps.CfgMgr))
	{
		auth.POST("/register", middleware.SecurityIPLimit(deps.CfgMgr, "register"), captchaMW, authHandler.Register)
		auth.POST("/login", middleware.SecurityIPLimit(deps.CfgMgr, "login"), captchaMW, authHandler.Login)
		auth.POST("/google", authHandler.GoogleOAuth)
		auth.POST("/refresh", authHandler.RefreshToken)
	}

	// --- 验证码前端配置接口 ---
	captchaHandler := handler.NewCaptchaHandler(cfg)
	v1.GET("/captcha/config", captchaHandler.Config)

	// --- 需鉴权的业务路由 ---
	authorized := v1.Group("")
	authorized.Use(middleware.Auth(cfg))
	authorized.Use(middleware.CircuitBreaker(deps.CfgMgr))

	// 修改密码（需鉴权）：旧密码校验通过后失效所有已签发 Token
	authorized.PUT("/user/password", authHandler.ChangePassword)

	// 用户模块
	userHandler := handler.NewUserHandler(deps.UserRepo)
	user := authorized.Group("/user")
	{
		user.GET("/profile", userHandler.GetProfile)
		user.PUT("/profile", userHandler.UpdateProfile)
		user.GET("/points", userHandler.GetPoints)
	}

	// 挂机模块（支持多设备同时挂机）
	idleHandler := handler.NewIdleHandler(deps.IdleSvc)
	idle := authorized.Group("/idle")
	{
		idle.POST("/start", idleHandler.Start)
		idle.POST("/heartbeat", idleHandler.Heartbeat)
		idle.POST("/stop", idleHandler.Stop)
		idle.POST("/stop-device", idleHandler.StopDevice)
		idle.GET("/status", idleHandler.Status)
		idle.GET("/records", idleHandler.Records)
	}

	// 任务模块
	taskHandler := handler.NewTaskHandler(deps.TaskSvc)
	tasks := authorized.Group("/tasks")
	{
		tasks.GET("", taskHandler.List)
		tasks.GET("/progress", taskHandler.Progress)
		tasks.POST("/:id/claim", taskHandler.Claim)
	}

	// 商城模块
	shopHandler := handler.NewShopHandler(deps.ShopSvc)
	shop := authorized.Group("/shop")
	{
		shop.GET("/items", shopHandler.Items)
		shop.POST("/redeem", shopHandler.Redeem)
		shop.GET("/orders", shopHandler.Orders)
		shop.GET("/orders/:id", shopHandler.OrderDetail)
		// 定时抢购
		shop.GET("/flash/activities", shopHandler.FlashActivities)
		shop.POST("/flash/redeem", shopHandler.FlashRedeem)
	}

	// --- 运营管理接口（需 X-Admin-Token，见 middleware.AdminToken / docs/21_定时抢购高并发方案） ---
	// 与普通用户 JWT 体系分离，用于抢购活动配置、预热、库存对账等运维操作。
	adminMW := middleware.AdminToken(cfg)
	admin := v1.Group("/admin", adminMW)
	flashAdmin := admin.Group("/flash")
	{
		flashAdmin.GET("/activities", shopHandler.ListFlashActivities)
		flashAdmin.GET("/activities/:id", shopHandler.FlashActivityDetail)
		flashAdmin.POST("/activities", shopHandler.CreateFlashActivity)
		flashAdmin.PUT("/activities/:id", shopHandler.UpdateFlashActivity)
		flashAdmin.POST("/activities/:id/end", shopHandler.EndFlashActivity)
		flashAdmin.POST("/activities/:id/warmup", shopHandler.WarmupFlashSale)
		flashAdmin.POST("/activities/:id/sync", shopHandler.SyncFlashSaleStock)
	}
	// 普通商品库存对账（运营，重置/事故后修复 Redis 预扣计数器），与 flash 的 sync 对应。
	admin.POST("/shop/items/:id/reconcile", shopHandler.ReconcileItemStock)

	return r
}
