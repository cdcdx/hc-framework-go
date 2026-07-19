package captcha

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

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
	ticket := ""
	randStr := ""
	if len(parts) == 2 {
		ticket = parts[0]
		randStr = parts[1]
	} else {
		ticket = token
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

// 编译期断言：TencentVerifier 满足 CaptchaProvider 接口。
var _ CaptchaProvider = (*TencentVerifier)(nil)
