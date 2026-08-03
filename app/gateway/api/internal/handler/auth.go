package handler

import (
	"net/http"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/logic"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/types"
	"github.com/cdcdx/hc-framework-go/common/response"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// RegisterHandler 邮箱注册
func RegisterHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.RegisterReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "invalid request body")
			return
		}
		l := logic.NewRegisterLogic(r.Context(), svcCtx)
		resp, err := l.Register(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// LoginHandler 邮箱登录
func LoginHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.LoginReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "invalid request body")
			return
		}
		l := logic.NewLoginLogic(r.Context(), svcCtx)
		resp, err := l.Login(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// GoogleOAuthHandler Google OAuth
func GoogleOAuthHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GoogleOAuthReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "code is required")
			return
		}
		l := logic.NewGoogleOAuthLogic(r.Context(), svcCtx)
		resp, err := l.GoogleOAuth(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// RefreshTokenHandler 刷新 Token
func RefreshTokenHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.RefreshTokenReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "refresh_token is required")
			return
		}
		l := logic.NewRefreshTokenLogic(r.Context(), svcCtx)
		resp, err := l.RefreshToken(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// ChangePasswordHandler 修改密码
func ChangePasswordHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.ChangePasswordReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "invalid request body")
			return
		}
		l := logic.NewChangePasswordLogic(r.Context(), svcCtx)
		resp, err := l.ChangePassword(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}
