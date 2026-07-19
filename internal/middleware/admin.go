package middleware

import (
	"github.com/gin-gonic/gin"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/pkg/response"
)

// AdminToken 运营管理接口鉴权中间件（保护 /api/v1/admin/*）。
//
// 校验请求头 X-Admin-Token 是否等于配置 admin.token。该令牌为运营侧共享密钥，
// 与用户 JWT 体系分离——普通用户令牌无法访问运营接口。
//
// 安全约束：admin.token 为空时，本中间件对全部请求返回 403，杜绝「未配置即裸奔」；
// 生产部署务必通过环境变量/密钥注入该令牌，并由网关或内网策略进一步收敛来源。
func AdminToken(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		if cfg.Admin.Token == "" {
			response.Forbidden(c, model.CodePermissionDenied)
			c.Abort()
			return
		}
		if c.GetHeader("X-Admin-Token") != cfg.Admin.Token {
			response.Forbidden(c, model.CodePermissionDenied)
			c.Abort()
			return
		}
		c.Next()
	}
}
