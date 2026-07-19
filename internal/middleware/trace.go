package middleware

import (
	"net/http"

	"github.com/cdcdx/hc-framework-go/internal/trace"
	"github.com/gin-gonic/gin"
)

const (
	HeaderTraceID = "X-Trace-Id"
)

// Trace 中间件：基于 OpenTelemetry 语义创建 server span，并透传 W3C traceparent。
// - 优先从上游 traceparent 提取父上下文；缺失时兼容旧版 X-Trace-Id。
// - 始终设置 X-Trace-Id 响应头，保证旧链路可继续按 trace_id 串联日志。
func Trace() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1. 提取/兼容上游追踪上下文
		ctx := trace.ExtractHTTPHeader(c.Request.Context(), c.Request.Header.Get)

		// 2. 起步 server span
		ctx, span := trace.DefaultTracer().Start(
			ctx,
			c.Request.Method+" "+c.FullPath(),
			trace.WithKind(trace.SpanKindServer),
		)
		c.Request = c.Request.WithContext(ctx)

		// 3. 兼容旧链路：设置 X-Trace-Id 与 context
		tidHex := span.TraceIDHex()
		c.Set("trace_id", tidHex)
		c.Header(HeaderTraceID, tidHex)

		c.Next()
		span.End()
	}
}

// Recovery Panic 恢复中间件
func Recovery() gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, recovered interface{}) {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
			"code":     10003,
			"message":  "internal server error",
			"trace_id": c.GetString("trace_id"),
		})
	})
}
