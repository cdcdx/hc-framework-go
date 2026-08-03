package captcha

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// TurnstileVerifier Cloudflare Turnstile 验证器
type TurnstileVerifier struct {
	siteKey   string
	secretKey string
	client    *http.Client
}

// NewTurnstileVerifier 创建 Turnstile 验证器
func NewTurnstileVerifier(siteKey, secretKey string) *TurnstileVerifier {
	return &TurnstileVerifier{
		siteKey:   siteKey,
		secretKey: secretKey,
		client:    &http.Client{Timeout: 10 * time.Second},
	}
}

// siteVerifyResp 通用站点验证响应
type siteVerifyResp struct {
	Success    bool     `json:"success"`
	ErrorCodes []string `json:"error-codes,omitempty"`
	Hostname   string   `json:"hostname,omitempty"`
}

// Verify 验证 token
func (v *TurnstileVerifier) Verify(token, remoteIP string) (bool, error) {
	data := url.Values{}
	data.Set("secret", v.secretKey)
	data.Set("response", token)
	if remoteIP != "" {
		data.Set("remoteip", remoteIP)
	}

	resp, err := v.client.PostForm(
		"https://challenges.cloudflare.com/turnstile/v0/siteverify",
		data,
	)
	if err != nil {
		return false, fmt.Errorf("turnstile verify: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var r siteVerifyResp
	if err := json.Unmarshal(body, &r); err != nil {
		return false, fmt.Errorf("turnstile parse: %w", err)
	}

	if !r.Success {
		return false, fmt.Errorf("turnstile failed: %v", r.ErrorCodes)
	}
	return true, nil
}

// GetScriptSrc 获取前端脚本地址
func (v *TurnstileVerifier) GetScriptSrc() string {
	return "https://challenges.cloudflare.com/turnstile/v0/api.js"
}

// ReCAPTCHAVerifier Google reCAPTCHA 验证器
type ReCAPTCHAVerifier struct {
	siteKey   string
	secretKey string
	client    *http.Client
}

// NewReCAPTCHAVerifier 创建 reCAPTCHA 验证器
func NewReCAPTCHAVerifier(siteKey, secretKey string) *ReCAPTCHAVerifier {
	return &ReCAPTCHAVerifier{
		siteKey:   siteKey,
		secretKey: secretKey,
		client:    &http.Client{Timeout: 10 * time.Second},
	}
}

// Verify 验证 token
func (v *ReCAPTCHAVerifier) Verify(token, remoteIP string) (bool, error) {
	data := url.Values{}
	data.Set("secret", v.secretKey)
	data.Set("response", token)
	if remoteIP != "" {
		data.Set("remoteip", remoteIP)
	}

	resp, err := v.client.PostForm(
		"https://www.google.com/recaptcha/api/siteverify",
		data,
	)
	if err != nil {
		return false, fmt.Errorf("recaptcha verify: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var r siteVerifyResp
	if err := json.Unmarshal(body, &r); err != nil {
		return false, fmt.Errorf("recaptcha parse: %w", err)
	}

	if !r.Success {
		return false, fmt.Errorf("recaptcha failed: %v", r.ErrorCodes)
	}
	return true, nil
}

// GetScriptSrc 获取前端脚本地址
func (v *ReCAPTCHAVerifier) GetScriptSrc() string {
	return fmt.Sprintf("https://www.google.com/recaptcha/api.js?render=%s", v.siteKey)
}

// hCAPTCHAVerifier hCaptcha 验证器
type hCAPTCHAVerifier struct {
	siteKey   string
	secretKey string
	client    *http.Client
}

// NewhCAPTCHAVerifier 创建 hCaptcha 验证器
func NewhCAPTCHAVerifier(siteKey, secretKey string) *hCAPTCHAVerifier {
	return &hCAPTCHAVerifier{
		siteKey:   siteKey,
		secretKey: secretKey,
		client:    &http.Client{Timeout: 10 * time.Second},
	}
}

// Verify 验证 token
func (v *hCAPTCHAVerifier) Verify(token, remoteIP string) (bool, error) {
	body := bytes.NewBufferString(fmt.Sprintf(
		"secret=%s&response=%s&remoteip=%s",
		v.secretKey, token, remoteIP,
	))

	resp, err := v.client.Post(
		"https://api.hcaptcha.com/siteverify",
		"application/x-www-form-urlencoded",
		body,
	)
	if err != nil {
		return false, fmt.Errorf("hcaptcha verify: %w", err)
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	var r siteVerifyResp
	if err := json.Unmarshal(b, &r); err != nil {
		return false, fmt.Errorf("hcaptcha parse: %w", err)
	}

	if !r.Success {
		return false, fmt.Errorf("hcaptcha failed: %v", r.ErrorCodes)
	}
	return true, nil
}

// GetScriptSrc 获取前端脚本地址
func (v *hCAPTCHAVerifier) GetScriptSrc() string {
	return "https://js.hcaptcha.com/1/api.js"
}

// TencentVerifier 腾讯云验证码验证器
type TencentVerifier struct {
	appID     string
	secretKey string
}

// NewTencentVerifier 创建腾讯云验证码验证器
func NewTencentVerifier(appID, secretKey string) *TencentVerifier {
	return &TencentVerifier{appID: appID, secretKey: secretKey}
}

// TencentVerifyResp 腾讯验证码响应
type TencentVerifyResp struct {
	Response  string `json:"response"` // "1" = 验证成功
	EvilLevel string `json:"evil_level"`
	ErrMsg    string `json:"err_msg"`
}

// Verify 验证 token（格式: ticket|randstr）
func (v *TencentVerifier) Verify(token, remoteIP string) (bool, error) {
	parts := strings.SplitN(token, "|", 2)
	ticket := token
	randStr := ""
	if len(parts) == 2 {
		ticket = parts[0]
		randStr = parts[1]
	}

	u, _ := url.Parse("https://ssl.captcha.qq.com/ticket/verify")
	q := url.Values{}
	q.Set("aid", v.appID)
	q.Set("AppSecretKey", v.secretKey)
	q.Set("Ticket", ticket)
	q.Set("Randstr", randStr)
	q.Set("UserIP", remoteIP)
	u.RawQuery = q.Encode()

	resp, err := http.Get(u.String())
	if err != nil {
		return false, fmt.Errorf("tencent captcha verify: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var r TencentVerifyResp
	if err := json.Unmarshal(body, &r); err != nil {
		return false, fmt.Errorf("tencent captcha parse: %w", err)
	}

	if r.Response != "1" {
		return false, fmt.Errorf("tencent captcha failed: %s", r.ErrMsg)
	}
	return true, nil
}

// GetScriptSrc 获取前端脚本地址
func (v *TencentVerifier) GetScriptSrc() string {
	return fmt.Sprintf("https://ssl.captcha.qq.com/TCaptcha.js?appid=%s", v.appID)
}

// 编译期断言：全部验证器均满足 CaptchaProvider 接口
var (
	_ CaptchaProvider = (*TurnstileVerifier)(nil)
	_ CaptchaProvider = (*ReCAPTCHAVerifier)(nil)
	_ CaptchaProvider = (*hCAPTCHAVerifier)(nil)
	_ CaptchaProvider = (*TencentVerifier)(nil)
)
