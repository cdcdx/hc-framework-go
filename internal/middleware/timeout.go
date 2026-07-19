package middleware

import (
	"context"
	"net/http"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/pkg/response"
	"github.com/gin-gonic/gin"
)

// defaultRequestTimeout 未配置时的兜底上限：防止请求（及其 DB/锁等待）无限挂起。
// 该值仅作为「绝不无限等待」的安全网；正常请求远低于此。若仍频繁触发，说明下游
// （DB 连接池 / 慢查询）成为瓶颈，应扩容或优化，而非调大此值（13 §3.48）。
const defaultRequestTimeout = 10 * time.Second

// Timeout 为请求上下文设置上限 deadline。
//
// 关键机理：下游 GORM / database/sql 查询会继承该 ctx；当连接池被打满（并发超过
// max_open_conns）时，database/sql 在等待空闲连接的过程中会监听 ctx；deadline 到达
// 即取消等待并返回 context.DeadlineExceeded，从而把「连接池饱和 → 请求无限等待空闲连接」
// 的 15~19s 长尾收敛为有界 504（13 §3.48）。
//
// 采用「同 goroutine 执行 c.Next()」而非独立 goroutine，确保前序的 Recovery 中间件
// 仍能在同一调用栈捕获 handler panic，避免超时协程逃逸导致进程崩溃。
func Timeout(cfg *config.Config) gin.HandlerFunc {
	d := cfg.Server.RequestTimeout
	if d <= 0 {
		d = defaultRequestTimeout
	}
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), d)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)

		c.Next()

		// 仅当确实因超时中断、且尚未写任何响应时，统一回 504。
		// 若 handler 已正常写回（哪怕是 ctx 取消后补写的错误响应），则不覆盖。
		if ctx.Err() == context.DeadlineExceeded && !c.Writer.Written() {
			response.ErrorWithHTTPStatus(c, http.StatusGatewayTimeout, model.CodeGatewayTimeout)
		}
	}
}
