package middleware

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/svc"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/response"
)

// CaptchaMiddleware 验证码防水墙：配置启用时，注册/登录请求必须携带 captcha_token 且通过校验。
// 读取请求体后恢复原 body，保证后续 handler 可正常绑定参数。
type CaptchaMiddleware struct {
	svcCtx *svc.ServiceContext
}

// NewCaptchaMiddleware 创建验证码中间件
func NewCaptchaMiddleware(svcCtx *svc.ServiceContext) *CaptchaMiddleware {
	return &CaptchaMiddleware{svcCtx: svcCtx}
}

// Handle 中间件处理
func (m *CaptchaMiddleware) Handle(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 未启用或未配置 provider 时直接放行
		if !m.svcCtx.Config.Captcha.Enabled || m.svcCtx.CaptchaProvider == nil {
			next(w, r)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			response.Forbidden(w, r, errorx.CodeCaptchaVerify)
			return
		}
		// 恢复 body 供后续解析
		r.Body = io.NopCloser(bytes.NewReader(body))

		var payload struct {
			CaptchaToken string `json:"captcha_token"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			// 畸形 JSON 视为未携带 token（fail-closed）；仅记 debug，避免刷日志。
			logx.WithContext(r.Context()).Debugf("captcha middleware: parse body failed: %v", err)
		}

		if payload.CaptchaToken == "" {
			response.Forbidden(w, r, errorx.CodeCaptchaVerify)
			return
		}

		ok, err := m.svcCtx.CaptchaProvider.Verify(payload.CaptchaToken, clientIP(r))
		if err != nil || !ok {
			response.Forbidden(w, r, errorx.CodeCaptchaVerify)
			return
		}
		next(w, r)
	}
}
