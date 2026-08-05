// Package response 统一 HTTP 响应封装（go-zero 版）。
// 响应结构（code/message/data/trace_id）与 gin 版完全一致，
// 客户端无需改动即可平滑切换。
package response

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/trace"
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

// getTraceID 从请求 context 中提取 TraceID（go-zero 中间件注入）
func getTraceID(r *http.Request) string {
	if r == nil {
		return ""
	}
	return trace.TraceIDFromContext(r.Context())
}

// resolveMsg 解析错误消息：优先自定义消息，否则回退错误码默认消息
func resolveMsg(code int, customMsg ...string) string {
	if len(customMsg) > 0 && customMsg[0] != "" {
		return customMsg[0]
	}
	return errorx.Message(code)
}

// writeJSON 写 JSON 响应
func writeJSON(w http.ResponseWriter, httpStatus int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(httpStatus)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// 客户端断开或编码失败时已无法改写状态码，至少记录日志便于排查。
		logx.Errorf("response.writeJSON: encode failed: %v", err)
	}
}

// Success 成功响应
func Success(w http.ResponseWriter, r *http.Request, data any) {
	writeJSON(w, http.StatusOK, Response{
		Code:    errorx.CodeSuccess,
		Message: errorx.Message(errorx.CodeSuccess),
		Data:    data,
		TraceID: getTraceID(r),
	})
}

// SuccessPage 分页成功响应
func SuccessPage(w http.ResponseWriter, r *http.Request, data any, nextCursor string, hasMore bool) {
	writeJSON(w, http.StatusOK, Response{
		Code:    errorx.CodeSuccess,
		Message: errorx.Message(errorx.CodeSuccess),
		Data: PageResponse{
			Items:      data,
			NextCursor: nextCursor,
			HasMore:    hasMore,
		},
		TraceID: getTraceID(r),
	})
}

// Error 错误响应（HTTP 200 + 业务错误码）
func Error(w http.ResponseWriter, r *http.Request, code int, customMsg ...string) {
	writeJSON(w, http.StatusOK, Response{
		Code:    code,
		Message: resolveMsg(code, customMsg...),
		TraceID: getTraceID(r),
	})
}

// ErrorWithHTTPStatus 带自定义 HTTP 状态码的错误响应
func ErrorWithHTTPStatus(w http.ResponseWriter, r *http.Request, httpStatus, code int, customMsg ...string) {
	writeJSON(w, httpStatus, Response{
		Code:    code,
		Message: resolveMsg(code, customMsg...),
		TraceID: getTraceID(r),
	})
}

// RateLimited 限流响应 (429)
func RateLimited(w http.ResponseWriter, r *http.Request, retryAfter int) {
	w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	ErrorWithHTTPStatus(w, r, http.StatusTooManyRequests, errorx.CodeRateLimited)
}

// ServiceUnavailable 服务不可用响应 (503)
func ServiceUnavailable(w http.ResponseWriter, r *http.Request, code int) {
	ErrorWithHTTPStatus(w, r, http.StatusServiceUnavailable, code)
}

// BadRequest 参数错误 (400)
func BadRequest(w http.ResponseWriter, r *http.Request, msg string) {
	ErrorWithHTTPStatus(w, r, http.StatusBadRequest, errorx.CodeInvalidParam, msg)
}

// Unauthorized 未授权 (401)
func Unauthorized(w http.ResponseWriter, r *http.Request, code int) {
	ErrorWithHTTPStatus(w, r, http.StatusUnauthorized, code)
}

// Forbidden 无权限 (403)
func Forbidden(w http.ResponseWriter, r *http.Request, code int) {
	ErrorWithHTTPStatus(w, r, http.StatusForbidden, code)
}
