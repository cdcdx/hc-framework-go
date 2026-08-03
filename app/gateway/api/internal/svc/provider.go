package svc

import "fmt"

// errUnknownCaptchaProvider 未知验证码提供商
func errUnknownCaptchaProvider(p string) error {
	return fmt.Errorf("unknown captcha provider: %s", p)
}
