// Package response 统一 HTTP 响应封装。
package response

import (
	"net/http"
	"strconv"

	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/gin-gonic/gin"
)

// Response 统一响应结构
type Response struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
	TraceID string `json:"trace_id,omitempty"`
}

// PageResponse 分页响应
type PageResponse struct {
	Items      any    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
	HasMore    bool   `json:"has_more"`
}

// getTraceID 从 context 中获取 TraceID
func getTraceID(c *gin.Context) string {
	if traceID, exists := c.Get("trace_id"); exists {
		if id, ok := traceID.(string); ok {
			return id
		}
	}
	return ""
}

// resolveMsg 解析错误消息：优先使用自定义消息，否则回退到错误码默认消息。
func resolveMsg(code int, customMsg ...string) string {
	if len(customMsg) > 0 && customMsg[0] != "" {
		return customMsg[0]
	}
	return model.Message(code)
}

// Success 成功响应
func Success(c *gin.Context, data any) {
	c.JSON(http.StatusOK, Response{
		Code:    model.CodeSuccess,
		Message: model.Message(model.CodeSuccess),
		Data:    data,
		TraceID: getTraceID(c),
	})
}

// SuccessPage 分页成功响应
func SuccessPage(c *gin.Context, data any, nextCursor string, hasMore bool) {
	c.JSON(http.StatusOK, Response{
		Code:    model.CodeSuccess,
		Message: model.Message(model.CodeSuccess),
		Data: PageResponse{
			Items:      data,
			NextCursor: nextCursor,
			HasMore:    hasMore,
		},
		TraceID: getTraceID(c),
	})
}

// Error 错误响应
func Error(c *gin.Context, code int, customMsg ...string) {
	c.JSON(http.StatusOK, Response{
		Code:    code,
		Message: resolveMsg(code, customMsg...),
		TraceID: getTraceID(c),
	})
}

// ErrorWithHTTPStatus 带自定义 HTTP 状态码的错误响应
func ErrorWithHTTPStatus(c *gin.Context, httpStatus int, code int, customMsg ...string) {
	c.JSON(httpStatus, Response{
		Code:    code,
		Message: resolveMsg(code, customMsg...),
		TraceID: getTraceID(c),
	})
}

// RateLimited 限流响应 (429)
func RateLimited(c *gin.Context, retryAfter int) {
	c.Header("Retry-After", strconv.Itoa(retryAfter))
	ErrorWithHTTPStatus(c, http.StatusTooManyRequests, model.CodeRateLimited)
}

// ServiceUnavailable 服务不可用响应 (503)
func ServiceUnavailable(c *gin.Context, code int) {
	ErrorWithHTTPStatus(c, http.StatusServiceUnavailable, code)
}

// BadRequest 参数错误 (400)
func BadRequest(c *gin.Context, msg string) {
	ErrorWithHTTPStatus(c, http.StatusBadRequest, model.CodeInvalidParam, msg)
}

// Unauthorized 未授权 (401)
func Unauthorized(c *gin.Context, code int) {
	ErrorWithHTTPStatus(c, http.StatusUnauthorized, code)
}

// Forbidden 无权限 (403)
func Forbidden(c *gin.Context, code int) {
	ErrorWithHTTPStatus(c, http.StatusForbidden, code)
}
