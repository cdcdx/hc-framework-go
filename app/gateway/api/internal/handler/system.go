package handler

import (
	"net/http"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/logic"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/svc"
	"github.com/cdcdx/hc-framework-go/common/response"
)

// HealthHandler 健康检查
func HealthHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic.NewHealthLogic(r.Context(), svcCtx)
		resp, err := l.Health()
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// ReadyHandler 就绪检查
func ReadyHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic.NewReadyLogic(r.Context(), svcCtx)
		resp, err := l.Ready()
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// CaptchaConfigHandler 验证码前端配置
func CaptchaConfigHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic.NewCaptchaConfigLogic(r.Context(), svcCtx)
		resp, err := l.CaptchaConfig()
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}
