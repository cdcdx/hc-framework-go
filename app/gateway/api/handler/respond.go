package handler

import (
	"net/http"

	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/response"
)

// handleError 把（可能跨 rpc 传递的）业务错误统一转为 HTTP 响应
func handleError(w http.ResponseWriter, r *http.Request, err error) {
	code := errorx.Code(err)
	msg := errorx.Msg(err)
	switch code {
	case errorx.CodeTokenInvalid, errorx.CodeTokenExpired, errorx.CodeTokenBlacklisted:
		response.Unauthorized(w, r, code)
	case errorx.CodePermissionDenied:
		response.Forbidden(w, r, code)
	case errorx.CodeInvalidParam:
		response.BadRequest(w, r, msg)
	case errorx.CodeRateLimited:
		response.RateLimited(w, r, 1)
	case errorx.CodeServiceUnavailable:
		response.ServiceUnavailable(w, r, code)
	default:
		response.Error(w, r, code, msg)
	}
}
