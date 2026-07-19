package middleware

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/pkg/logger"
)

// sensitivePaths 不记录请求体的敏感路径（含密码等凭证）。
var sensitivePaths = []string{"/login", "/register"}

func isSensitivePath(path string) bool {
	for _, p := range sensitivePaths {
		if strings.HasSuffix(path, p) {
			return true
		}
	}
	return false
}

// RequestLogger 请求日志中间件
//   - 敏感路径（/login、/register）不记录请求体，避免凭证泄露；
//   - 其它 JSON 写请求记录请求体：超 Logging.RequestBodyMaxSize（默认 4KB）截断，
//     并对敏感字段（password/token/secret/authorization 等，取自配置）脱敏为 ***；
//   - 读取后会还原 Body，不影响后续中间件/处理器绑定。
func RequestLogger(cfg *config.Config) gin.HandlerFunc {
	zlog := logger.L()

	sensitive := make(map[string]bool, len(cfg.Logging.SensitiveFields))
	for _, f := range cfg.Logging.SensitiveFields {
		sensitive[strings.ToLower(f)] = true
	}
	maxBody := cfg.Logging.RequestBodyMaxSize
	if maxBody <= 0 {
		maxBody = 4096
	}

	return func(c *gin.Context) {
		start := time.Now()

		// 提前读取并记录（非敏感路径的 JSON 写请求）请求体；读取后还原 Body
		var bodyStr string
		method := c.Request.Method
		ct := c.Request.Header.Get("Content-Type")
		if !isSensitivePath(c.Request.URL.Path) &&
			(method == "POST" || method == "PUT" || method == "PATCH") &&
			strings.Contains(ct, "json") {
			if raw, ok := readBody(c); ok {
				logRaw := raw
				if len(logRaw) > maxBody {
					logRaw = logRaw[:maxBody]
				}
				bodyStr = maskJSON(logRaw, sensitive)
			}
		}

		c.Next()

		duration := time.Since(start)
		fields := []zapcore.Field{
			zap.String("trace_id", c.GetString("trace_id")),
			zap.String("method", method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.String("ip", c.ClientIP()),
			zap.Duration("duration_ms", duration),
			zap.Int("body_size", c.Writer.Size()),
		}
		if bodyStr != "" {
			fields = append(fields, zap.String("body", bodyStr))
		}

		switch {
		case c.Writer.Status() >= 500:
			zlog.Error("request completed", fields...)
		case c.Writer.Status() >= 400:
			zlog.Warn("request completed", fields...)
		default:
			zlog.Info("request completed", fields...)
		}
	}
}

// readBody 读取完整请求体后还原 Body，供后续中间件/处理器继续使用。
func readBody(c *gin.Context) ([]byte, bool) {
	if c.Request.Body == nil {
		return nil, false
	}
	raw, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return nil, false
	}
	c.Request.Body = io.NopCloser(bytes.NewReader(raw))
	return raw, true
}

// maskJSON 对 JSON 请求体中的敏感字段脱敏；非 JSON 原样返回。
func maskJSON(raw []byte, sensitive map[string]bool) string {
	var data map[string]interface{}
	if err := json.Unmarshal(raw, &data); err != nil {
		return string(raw)
	}
	for k := range data {
		if sensitive[strings.ToLower(k)] {
			data[k] = "***"
		}
	}
	masked, err := json.Marshal(data)
	if err != nil {
		return string(raw)
	}
	return string(masked)
}
