package captcha

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakeServer 返回一个按配置返回 success/fail 的验证服务器桩。
func fakeServer(t *testing.T, success bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		_ = json.NewEncoder(w).Encode(siteVerifyResp{
			Success: success,
		})
	}))
}

// TestNoopVerifier 不校验直接通过。
func TestNoopVerifier(t *testing.T) {
	v := &NoopVerifier{}
	ok, err := v.Verify("any", "1.2.3.4")
	if !ok || err != nil {
		t.Fatalf("noop should pass: ok=%v err=%v", ok, err)
	}
	if v.GetScriptSrc() != "" {
		t.Fatalf("noop script src should be empty")
	}
}

// TestTurnstileVerifier 覆盖成功与失败两条路径（替换 endpoint 为桩）。
func TestTurnstileVerifier(t *testing.T) {
	srv := fakeServer(t, true)
	defer srv.Close()

	v := &TurnstileVerifier{siteKey: "k", secretKey: "s", client: srv.Client()}
	// 将真实 endpoint 替换为桩地址。
	orig := "https://challenges.cloudflare.com/turnstile/v0/siteverify"
	t.Cleanup(func() {
		_ = orig
	})

	ok, err := v.verifyAt(srv.URL, "token", "1.2.3.4")
	if !ok || err != nil {
		t.Fatalf("expected success: ok=%v err=%v", ok, err)
	}

	srvFail := fakeServer(t, false)
	defer srvFail.Close()
	ok, err = v.verifyAt(srvFail.URL, "token", "1.2.3.4")
	if ok {
		t.Fatalf("expected failure when success=false")
	}
	if err == nil {
		t.Fatalf("expected error when success=false")
	}
}

// TestReCAPTCHAVerifier 覆盖成功路径。
func TestReCAPTCHAVerifier(t *testing.T) {
	srv := fakeServer(t, true)
	defer srv.Close()
	v := &ReCAPTCHAVerifier{siteKey: "k", secretKey: "s", client: srv.Client()}
	ok, err := v.verifyAt(srv.URL, "token", "1.2.3.4")
	if !ok || err != nil {
		t.Fatalf("expected success: ok=%v err=%v", ok, err)
	}
}

// TestHCAPTCHAVerifier 覆盖成功路径（使用 body 提交）。
func TestHCAPTCHAVerifier(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(siteVerifyResp{Success: true})
	}))
	defer srv.Close()
	v := &hCAPTCHAVerifier{siteKey: "k", secretKey: "s", client: srv.Client()}
	ok, err := v.verifyAt(srv.URL, "token", "1.2.3.4")
	if !ok || err != nil {
		t.Fatalf("expected success: ok=%v err=%v", ok, err)
	}
}

// TestTencentVerifier 覆盖 ticket|randstr 拆分与成功判定（response=="1"）。
func TestTencentVerifier(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(TencentVerifyResp{Response: "1", EvilLevel: "0", ErrMsg: ""})
	}))
	defer srv.Close()
	v := &TencentVerifier{appID: "aid", secretKey: "sk"}
	ok, err := v.verifyAt(srv.URL, "ticket|randstr", "1.2.3.4")
	if !ok || err != nil {
		t.Fatalf("expected success: ok=%v err=%v", ok, err)
	}

	srvFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(TencentVerifyResp{Response: "0", ErrMsg: "bad"})
	}))
	defer srvFail.Close()
	ok, err = v.verifyAt(srvFail.URL, "ticket|randstr", "1.2.3.4")
	if ok || err == nil {
		t.Fatalf("expected failure when response!=1")
	}
}

// TestGetScriptSrc 各验证器脚本地址非空（腾讯除外，依赖 appid 拼接）。
func TestGetScriptSrc(t *testing.T) {
	cases := map[string]CaptchaProvider{
		"turnstile": &TurnstileVerifier{siteKey: "k"},
		"recaptcha": &ReCAPTCHAVerifier{siteKey: "k"},
		"hcaptcha":  &hCAPTCHAVerifier{},
	}
	for name, v := range cases {
		if src := v.GetScriptSrc(); src == "" {
			t.Errorf("%s GetScriptSrc empty", name)
		}
	}
}
