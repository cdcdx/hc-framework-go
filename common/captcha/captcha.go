// Package captcha 验证码相关能力（go-zero 版，与 gin 版实现一致）。
package captcha

// CaptchaProvider 验证码提供商接口
type CaptchaProvider interface {
	// Verify 验证 token
	Verify(token, remoteIP string) (bool, error)
	// GetScriptSrc 获取前端脚本地址
	GetScriptSrc() string
}

// NoopVerifier 空验证器：不校验，直接通过
type NoopVerifier struct{}

func (n *NoopVerifier) Verify(token, remoteIP string) (bool, error) { return true, nil }
func (n *NoopVerifier) GetScriptSrc() string                        { return "" }

var _ CaptchaProvider = (*NoopVerifier)(nil)
