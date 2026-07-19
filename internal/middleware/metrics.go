package middleware

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/cdcdx/hc-framework-go/internal/metrics"
)

// Metrics 采集 HTTP 请求计数与延迟的 Prometheus 指标中间件。
// 需在路由匹配后读取 FullPath，故放在全局链中（c.Next 之后取值）。
func Metrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		path := c.FullPath()
		if path == "" {
			// 未匹配到路由模板的请求（如 404 兜底）不回退到原始 URL，
			// 否则带路径参数的 URL 会作为 label 值造成基数爆炸。统一归并为 "unknown"，
			// 仍可由 status 维度观测，且不在时序库里产生海量高基数序列。
			path = "unknown"
		}
		status := strconv.Itoa(c.Writer.Status())

		metrics.HTTPRequestsTotal.WithLabelValues(c.Request.Method, path, status).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(c.Request.Method, path).Observe(time.Since(start).Seconds())
	}
}
