// Package handler HTTP 路由注册与处理器（goctl 生成 routes.go 的手写等价版）。
package handler

import (
	"net/http"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/middleware"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/svc"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
			// Prometheus 拉取端点：不走任何中间件（含 JWT），供 scrape 直接访问
			{Method: http.MethodGet, Path: "/metrics", Handler: func(w http.ResponseWriter, r *http.Request) {
				promhttp.Handler().ServeHTTP(w, r)
			}},
		},
	)

	// 防水墙组：注册/登录（IP 限频 → 验证码）
	server.AddRoutes(
		rest.WithMiddlewares(
			[]rest.Middleware{
				middleware.NewSecurityIPLimitMiddleware(serverCtx).Handle,
				middleware.NewCaptchaMiddleware(serverCtx).Handle,
			},
			rest.Route{Method: http.MethodPost, Path: "/auth/register", Handler: RegisterHandler(serverCtx)},
			rest.Route{Method: http.MethodPost, Path: "/auth/login", Handler: LoginHandler(serverCtx)},
		),
		rest.WithPrefix("/api/v1"),
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
		rest.WithMiddlewares(
			[]rest.Middleware{
				middleware.NewJwtAuthMiddleware(serverCtx).Handle,
			},
			rest.Route{Method: http.MethodPut, Path: "/user/password", Handler: ChangePasswordHandler(serverCtx)},
			rest.Route{Method: http.MethodGet, Path: "/user/profile", Handler: GetProfileHandler(serverCtx)},
			rest.Route{Method: http.MethodPut, Path: "/user/profile", Handler: UpdateProfileHandler(serverCtx)},
			rest.Route{Method: http.MethodGet, Path: "/user/points", Handler: GetPointsHandler(serverCtx)},

			rest.Route{Method: http.MethodPost, Path: "/idle/start", Handler: IdleStartHandler(serverCtx)},
			rest.Route{Method: http.MethodPost, Path: "/idle/heartbeat", Handler: IdleHeartbeatHandler(serverCtx)},
			rest.Route{Method: http.MethodPost, Path: "/idle/stop", Handler: IdleStopHandler(serverCtx)},
			rest.Route{Method: http.MethodPost, Path: "/idle/stop-device", Handler: IdleStopDeviceHandler(serverCtx)},
			rest.Route{Method: http.MethodGet, Path: "/idle/status", Handler: IdleStatusHandler(serverCtx)},
			rest.Route{Method: http.MethodGet, Path: "/idle/records", Handler: IdleRecordsHandler(serverCtx)},

			rest.Route{Method: http.MethodGet, Path: "/tasks", Handler: TaskListHandler(serverCtx)},
			rest.Route{Method: http.MethodGet, Path: "/tasks/progress", Handler: TaskProgressHandler(serverCtx)},
			rest.Route{Method: http.MethodPost, Path: "/tasks/:id/claim", Handler: TaskClaimHandler(serverCtx)},

			rest.Route{Method: http.MethodGet, Path: "/shop/items", Handler: ShopItemsHandler(serverCtx)},
			rest.Route{Method: http.MethodPost, Path: "/shop/redeem", Handler: ShopRedeemHandler(serverCtx)},
			rest.Route{Method: http.MethodGet, Path: "/shop/orders", Handler: ShopOrdersHandler(serverCtx)},
			rest.Route{Method: http.MethodGet, Path: "/shop/orders/:id", Handler: ShopOrderDetailHandler(serverCtx)},
			rest.Route{Method: http.MethodGet, Path: "/shop/flash/activities", Handler: ShopFlashActivitiesHandler(serverCtx)},
			rest.Route{Method: http.MethodPost, Path: "/shop/flash/redeem", Handler: ShopFlashRedeemHandler(serverCtx)},
		),
		rest.WithPrefix("/api/v1"),
	)
}
