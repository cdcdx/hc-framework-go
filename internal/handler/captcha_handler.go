package handler

import (
	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/pkg/response"
	"github.com/gin-gonic/gin"
)

// CaptchaHandler 验证码处理器
type CaptchaHandler struct {
	cfg *config.Config
}

// NewCaptchaHandler 创建验证码处理器
func NewCaptchaHandler(cfg *config.Config) *CaptchaHandler {
	return &CaptchaHandler{cfg: cfg}
}

// Config 返回验证码前端配置
// GET /api/v1/captcha/config
func (h *CaptchaHandler) Config(c *gin.Context) {
	cc := h.cfg.Captcha
	resp := gin.H{
		"type": cc.Type,
	}

	switch cc.Type {
	case "tencent":
		resp["app_id"] = cc.Tencent.AppID
		resp["script_src"] = "https://ssl.captcha.qq.com/TCaptcha.js"
	case "turnstile":
		resp["site_key"] = cc.Turnstile.SiteKey
		resp["script_src"] = "https://challenges.cloudflare.com/turnstile/v0/api.js"
	case "recaptcha":
		resp["site_key"] = cc.ReCAPTCHA.SiteKey
		resp["script_src"] = "https://www.google.com/recaptcha/api.js"
	case "hcaptcha":
		resp["site_key"] = cc.HCAPTCHA.SiteKey
		resp["script_src"] = "https://js.hcaptcha.com/1/api.js"
	default:
		resp["enabled"] = false
	}

	resp["trigger"] = gin.H{
		"email_per_minute": cc.Trigger.EmailPerMinute,
		"ip_per_minute":    cc.Trigger.IPPerMinute,
	}
	resp["whitelist_ttl_seconds"] = int(cc.WhitelistTTL.Seconds())

	response.Success(c, resp)
}
