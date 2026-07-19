package middleware

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/cdcdx/hc-framework-go/pkg/contextkeys"
)

// RequestInfo 注入请求信息（IP、UserAgent）到 context，供日志/监控使用
func RequestInfo() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		ctx = context.WithValue(ctx, contextkeys.ClientIP, c.ClientIP())
		ctx = context.WithValue(ctx, contextkeys.UserAgent, c.Request.UserAgent())
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}
