// Package handler HTTP 处理层，负责路由、参数绑定与响应。
package handler

import (
	"github.com/cdcdx/hc-framework-go/internal/service/auth"
	"errors"

	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/pkg/response"
	"github.com/gin-gonic/gin"
)

// AuthHandler 认证处理器
type AuthHandler struct {
	svc *auth.AuthService
}

// NewAuthHandler 创建认证处理器
func NewAuthHandler(svc *auth.AuthService) *AuthHandler {
	return &AuthHandler{svc: svc}
}

// Register 邮箱注册
// @Summary      邮箱注册
// @Description  使用邮箱和密码注册新账号
// @Tags         认证
// @Accept       json
// @Produce      json
// @Param        body  body      object{email=string,password=string,nickname=string}  true  "注册信息"
// @Success      200   {object}  map[string]interface{}
// @Failure      400   {object}  map[string]interface{}
// @Router       /api/v1/auth/register [post]
func (h *AuthHandler) Register(c *gin.Context) {
	var req auth.RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "invalid request body")
		return
	}

	user, err := h.svc.Register(c.Request.Context(), &req)
	if err != nil {
		switch err {
		case auth.ErrEmailRegistered:
			response.Error(c, model.CodeEmailRegistered)
		default:
			response.Error(c, model.CodeUnknownError, err.Error())
		}
		return
	}

	// 注册成功直接签发 JWT：复用 IssueTokens，避免再调一次 Login 重复做
	// bcrypt 校验 + FindByEmail（P0-1 修复：原实现让一次 /register 承担
	// 1 次 Hash + 1 次 Compare + 2 次查库，是 register 延迟翻倍的根因）。
	result, err := h.svc.IssueTokens(user)
	if err != nil {
		response.Error(c, model.CodeUnknownError, err.Error())
		return
	}

	response.Success(c, gin.H{
		"user":          user,
		"access_token":  result.AccessToken,
		"refresh_token": result.RefreshToken,
	})
}

// Login 邮箱登录
// @Summary      邮箱登录
// @Description  使用邮箱和密码登录，返回 JWT Token
// @Tags         认证
// @Accept       json
// @Produce      json
// @Param        body  body      object{email=string,password=string}  true  "登录信息"
// @Success      200   {object}  map[string]interface{}
// @Failure      401   {object}  map[string]interface{}
// @Router       /api/v1/auth/login [post]
func (h *AuthHandler) Login(c *gin.Context) {
	var req auth.LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "invalid request body")
		return
	}

	result, err := h.svc.Login(c.Request.Context(), &req)
	if err != nil {
		switch err {
		case auth.ErrInvalidCredentials:
			response.Error(c, model.CodePasswordWrong)
		case auth.ErrAccountLocked:
			response.Error(c, model.CodeAccountLocked)
		default:
			response.Error(c, model.CodeUnknownError, err.Error())
		}
		return
	}

	response.Success(c, gin.H{
		"user":          result.User,
		"access_token":  result.AccessToken,
		"refresh_token": result.RefreshToken,
	})
}

// ChangePassword 修改密码
// @Summary      修改密码
// @Description  校验旧密码后更新密码，并使所有已签发 Token 立即失效
// @Tags         认证
// @Security     BearerAuth
// @Accept       json
// @Produce      json
// @Param        body  body      auth.ChangePasswordRequest  true  "旧密码与新密码"
// @Success      200   {object}  map[string]interface{}
// @Failure      401   {object}  map[string]interface{}
// @Router       /api/v1/user/password [put]
func (h *AuthHandler) ChangePassword(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	var req auth.ChangePasswordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "invalid request body")
		return
	}

	if err := h.svc.ChangePassword(c.Request.Context(), userID, req.OldPassword, req.NewPassword); err != nil {
		switch err {
		case auth.ErrInvalidCredentials:
			response.Error(c, model.CodePasswordWrong, "old password incorrect")
		default:
			response.Error(c, model.CodeUnknownError, err.Error())
		}
		return
	}

	response.Success(c, gin.H{"message": "password changed"})
}

// GoogleOAuth Google OAuth 回调
// @Summary      Google OAuth 登录
// @Description  通过 Google OAuth 授权码登录/注册
// @Tags         认证
// @Accept       json
// @Produce      json
// @Param        body  body      object{code=string}  true  "Google 授权码"
// @Success      200   {object}  map[string]interface{}
// @Router       /api/v1/auth/google [post]
func (h *AuthHandler) GoogleOAuth(c *gin.Context) {
	var req struct {
		Code string `json:"code" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "code is required")
		return
	}

	// 用授权码向 Google 交换 token 并拉取用户信息，再登录/注册
	result, err := h.svc.GoogleOAuthCode(c.Request.Context(), req.Code)
	if err != nil {
		if errors.Is(err, auth.ErrOAuthFailed) {
			response.Error(c, model.CodeThirdPartyError, "google oauth failed")
			return
		}
		response.Error(c, model.CodeUnknownError, err.Error())
		return
	}

	response.Success(c, gin.H{
		"user":          result.User,
		"access_token":  result.AccessToken,
		"refresh_token": result.RefreshToken,
	})
}

// RefreshToken 刷新 Token
// @Summary      刷新 Token
// @Description  使用 Refresh Token 获取新的 Access Token
// @Tags         认证
// @Accept       json
// @Produce      json
// @Param        body  body      object{refresh_token=string}  true  "Refresh Token"
// @Success      200   {object}  map[string]interface{}
// @Failure      401   {object}  map[string]interface{}
// @Router       /api/v1/auth/refresh [post]
func (h *AuthHandler) RefreshToken(c *gin.Context) {
	var req struct {
		RefreshToken string `json:"refresh_token" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "refresh_token is required")
		return
	}

	result, err := h.svc.RefreshToken(c.Request.Context(), req.RefreshToken)
	if err != nil {
		response.Unauthorized(c, model.CodeTokenInvalid)
		return
	}

	response.Success(c, gin.H{
		"access_token":  result.AccessToken,
		"refresh_token": result.RefreshToken,
	})
}
