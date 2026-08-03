// Package handler HTTP 路由注册与处理器（goctl 生成 routes.go 的手写等价版）。
package handler

import (
	"net/http"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/middleware"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/svc"
	"github.com/zeromicro/go-zero/rest"
)

// RegisterHandlers 注册全部路由（分组与 gateway.api 中间件声明一致）
func RegisterHandlers(server *rest.Server, serverCtx *svc.ServiceContext) {
	// 公开接口
	server.AddRoutes(
		[]rest.Route{
			{Method: http.MethodGet, Path: "/health", Handler: HealthHandler(serverCtx)},
			{Method: http.MethodGet, Path: "/ready", Handler: ReadyHandler(serverCtx)},
			{Method: http.MethodGet, Path: "/api/v1/captcha/config", Handler: CaptchaConfigHandler(serverCtx)},
			{Method: http.MethodPost, Path: "/api/v1/auth/refresh", Handler: RefreshTokenHandler(serverCtx)},
		},
	)

	// 防水墙组：注册/登录（IP 限频 → 验证码）
	server.AddRoutes(
		[]rest.Route{
			{Method: http.MethodPost, Path: "/auth/register", Handler: RegisterHandler(serverCtx)},
			{Method: http.MethodPost, Path: "/auth/login", Handler: LoginHandler(serverCtx)},
		},
		rest.WithPrefix("/api/v1"),
		rest.WithMiddlewares(
			[]rest.Middleware{
				middleware.NewSecurityIPLimitMiddleware(serverCtx).Handle,
				middleware.NewCaptchaMiddleware(serverCtx).Handle,
			},
		),
	)

	// Google OAuth（无验证码）
	server.AddRoutes(
		[]rest.Route{
			{Method: http.MethodPost, Path: "/auth/google", Handler: GoogleOAuthHandler(serverCtx)},
		},
		rest.WithPrefix("/api/v1"),
	)

	// 鉴权业务组
	server.AddRoutes(
		[]rest.Route{
			{Method: http.MethodPut, Path: "/user/password", Handler: ChangePasswordHandler(serverCtx)},
			{Method: http.MethodGet, Path: "/user/profile", Handler: GetProfileHandler(serverCtx)},
			{Method: http.MethodPut, Path: "/user/profile", Handler: UpdateProfileHandler(serverCtx)},
			{Method: http.MethodGet, Path: "/user/points", Handler: GetPointsHandler(serverCtx)},

			{Method: http.MethodPost, Path: "/idle/start", Handler: IdleStartHandler(serverCtx)},
			{Method: http.MethodPost, Path: "/idle/heartbeat", Handler: IdleHeartbeatHandler(serverCtx)},
			{Method: http.MethodPost, Path: "/idle/stop", Handler: IdleStopHandler(serverCtx)},
			{Method: http.MethodPost, Path: "/idle/stop-device", Handler: IdleStopDeviceHandler(serverCtx)},
			{Method: http.MethodGet, Path: "/idle/status", Handler: IdleStatusHandler(serverCtx)},
			{Method: http.MethodGet, Path: "/idle/records", Handler: IdleRecordsHandler(serverCtx)},

			{Method: http.MethodGet, Path: "/tasks", Handler: TaskListHandler(serverCtx)},
			{Method: http.MethodGet, Path: "/tasks/progress", Handler: TaskProgressHandler(serverCtx)},
			{Method: http.MethodPost, Path: "/tasks/:id/claim", Handler: TaskClaimHandler(serverCtx)},

			{Method: http.MethodGet, Path: "/shop/items", Handler: ShopItemsHandler(serverCtx)},
			{Method: http.MethodPost, Path: "/shop/redeem", Handler: ShopRedeemHandler(serverCtx)},
			{Method: http.MethodGet, Path: "/shop/orders", Handler: ShopOrdersHandler(serverCtx)},
			{Method: http.MethodGet, Path: "/shop/orders/:id", Handler: ShopOrderDetailHandler(serverCtx)},
			{Method: http.MethodGet, Path: "/shop/flash/activities", Handler: ShopFlashActivitiesHandler(serverCtx)},
			{Method: http.MethodPost, Path: "/shop/flash/redeem", Handler: ShopFlashRedeemHandler(serverCtx)},
		},
		rest.WithPrefix("/api/v1"),
		rest.WithMiddlewares(
			[]rest.Middleware{
				middleware.NewJwtAuthMiddleware(serverCtx).Handle,
			},
		),
	)
}
