package handler

import (
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/pkg/response"
	"github.com/gin-gonic/gin"
)

// anonymousUser 是未鉴权/测试兜底的 user_id；命中时视为未登录。
const anonymousUser = "anonymous"

// requireUser 从 gin.Context 提取已鉴权的 user_id。
// 鉴权通过返回 (userID, true)；否则已写入 401 响应并返回 ("", false)，调用方应直接 return。
// 统一管理「空串 / anonymous」两种未登录情形，消除各 handler 重复的鉴权样板。
func requireUser(c *gin.Context) (string, bool) {
	userID := c.GetString("user_id")
	if userID == "" || userID == anonymousUser {
		response.Unauthorized(c, model.CodeTokenInvalid)
		return "", false
	}
	return userID, true
}
