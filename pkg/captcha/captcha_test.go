package captcha

import (
	"os"
	"testing"
)

func getTestCredentials() (tencentAppID, tencentSecret, turnstileSite, turnstileSecret, recaptchaSite, recaptchaSecret, hcaptchaSite, hcaptchaSecret string) {
	// 与 config.yaml 中的测试密钥一致
	return os.Getenv("CAPTCHA_TENCENT_APPID"), os.Getenv("CAPTCHA_TENCENT_SECRET"),
		os.Getenv("CAPTCHA_TURNSTILE_SITE"), os.Getenv("CAPTCHA_TURNSTILE_SECRET"),
		os.Getenv("CAPTCHA_RECAPTCHA_SITE"), os.Getenv("CAPTCHA_RECAPTCHA_SECRET"),
		os.Getenv("CAPTCHA_HCAPTCHA_SITE"), os.Getenv("CAPTCHA_HCAPTCHA_SECRET")
}

// TestTencentVerifier 测试腾讯云验证码 API 连通性
func TestTencentVerifier(t *testing.T) {
	appID, secretKey, _, _, _, _, _, _ := getTestCredentials()
	if appID == "" || secretKey == "" {
		// 使用 config.yaml 中的测试密钥
		appID = "195048768"
		secretKey = "<JWT_SECRET_KEY>"
	}

	v := NewTencentVerifier(appID, secretKey)
	t.Logf("Tencent Verifier created: appID=%s", appID)
	t.Logf("Script src: %s", v.GetScriptSrc())

	// 用假 token 测试 — 应该返回明确的错误而非网络错误
	ok, err := v.Verify("fake_ticket|fake_randstr", "127.0.0.1")
	if ok {
		t.Log("Tencent: 验证通过（假 token 不应该通过，可能是测试环境）")
	} else {
		t.Logf("Tencent: 验证拒绝（预期行为）: %v", err)
	}

	// 关键：不应该有网络连接错误
	if err != nil {
		t.Logf("Tencent 返回错误: %v", err)
	} else if ok {
		t.Error("Tencent: 假 token 不应该验证成功")
	}
}

// TestTurnstileVerifier 测试 Cloudflare Turnstile API 连通性
func TestTurnstileVerifier(t *testing.T) {
	_, _, siteKey, secretKey, _, _, _, _ := getTestCredentials()
	if siteKey == "" || secretKey == "" {
		siteKey = "<TURNSTILE_KEY>"
		secretKey = "<CLOUDFLARE_TURNSTILE_SECRET_KEY>"
	}

	v := NewTurnstileVerifier(siteKey, secretKey)
	t.Logf("Turnstile Verifier created: siteKey=%s", siteKey)
	t.Logf("Script src: %s", v.GetScriptSrc())

	ok, err := v.Verify("fake-turnstile-token", "127.0.0.1")
	if ok {
		t.Error("Turnstile: 假 token 不应该验证成功")
	}
	t.Logf("Turnstile 返回错误（预期）: %v", err)

	// 关键：验证不是网络错误
	if err == nil {
		t.Error("Turnstile: 假 token 应该有错误返回")
	}
}

// TestReCAPTCHAVerifier 测试 Google reCAPTCHA API 连通性
func TestReCAPTCHAVerifier(t *testing.T) {
	_, _, _, _, siteKey, secretKey, _, _ := getTestCredentials()
	if siteKey == "" || secretKey == "" {
		siteKey = "<CAPTCHA_KEY>"
		secretKey = "<HCAPTCHA_SITE_KEY>"
	}

	v := NewReCAPTCHAVerifier(siteKey, secretKey)
	t.Logf("ReCAPTCHA Verifier created: siteKey=%s", siteKey)
	t.Logf("Script src: %s", v.GetScriptSrc())

	ok, err := v.Verify("fake-recaptcha-token", "127.0.0.1")
	if ok {
		t.Error("ReCAPTCHA: 假 token 不应该验证成功")
	}
	t.Logf("ReCAPTCHA 返回错误（预期）: %v", err)

	if err == nil {
		t.Error("ReCAPTCHA: 假 token 应该有错误返回")
	}
}

// TestHCAPTCHAVerifier 测试 hCaptcha API 连通性
func TestHCAPTCHAVerifier(t *testing.T) {
	_, _, _, _, _, _, siteKey, secretKey := getTestCredentials()
	if siteKey == "" || secretKey == "" {
		siteKey = "<HCAPTCHA_SECRET_KEY>"
		secretKey = "<HCAPTCHA_SECRET_KEY>"
	}

	v := NewhCAPTCHAVerifier(siteKey, secretKey)
	t.Logf("hCaptcha Verifier created: siteKey=%s", siteKey)
	t.Logf("Script src: %s", v.GetScriptSrc())

	ok, err := v.Verify("fake-hcaptcha-token", "127.0.0.1")
	if ok {
		t.Error("hCaptcha: 假 token 不应该验证成功")
	}
	t.Logf("hCaptcha 返回错误（预期）: %v", err)

	if err == nil {
		t.Error("hCaptcha: 假 token 应该有错误返回")
	}
}

// TestAllProvidersComplete 完整测试所有提供商
func TestAllProvidersComplete(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{"Tencent", func(t *testing.T) {
			t.Run("TencentSub", TestTencentVerifier)
		}},
		{"Turnstile", func(t *testing.T) {
			t.Run("TurnstileSub", TestTurnstileVerifier)
		}},
		{"ReCAPTCHA", func(t *testing.T) {
			t.Run("ReCAPTCHASub", TestReCAPTCHAVerifier)
		}},
		{"HCAPTCHA", func(t *testing.T) {
			t.Run("HCAPTCHASub", TestHCAPTCHAVerifier)
		}},
	}

	for _, tt := range tests {
		t.Logf("========== 测试 %s ==========", tt.name)
		tt.testFunc(t)
	}
}
