// Package captcha 验证码相关能力。
package captcha

// CaptchaProvider 验证码提供商接口
type CaptchaProvider interface {
	// Verify 验证 token
	Verify(token, remoteIP string) (bool, error)
	// GetScriptSrc 获取前端脚本地址
	GetScriptSrc() string
}
