package middleware

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/pkg/captcha"
	"github.com/gin-gonic/gin"
)

func init() { gin.SetMode(gin.TestMode) }

// ───────────────────────── 滑动窗口 ─────────────────────────

// TestSlidingWindow_RecordAndCount 验证 record 累积、count 返回当前窗口内计数。
func TestSlidingWindow_RecordAndCount(t *testing.T) {
	sw := &slidingWindow{counters: make(map[string][]time.Time)}
	for i := 0; i < 3; i++ {
		sw.record("k")
	}
	if n := sw.count("k"); n != 3 {
		t.Fatalf("count(k) = %d, want 3", n)
	}
	// 不同 key 互不影响
	if n := sw.count("other"); n != 0 {
		t.Fatalf("count(other) = %d, want 0", n)
	}
}

// TestSlidingWindow_CountExcludesExpired 验证超过 1 分钟的旧时间戳被窗口剔除。
func TestSlidingWindow_CountExcludesExpired(t *testing.T) {
	now := time.Now()
	sw := &slidingWindow{counters: make(map[string][]time.Time)}
	sw.counters["k"] = []time.Time{
		now.Add(-2 * time.Minute),  // 过期（早于 cutoff）
		now.Add(-30 * time.Second), // 有效
		now,                        // 有效
	}
	if n := sw.count("k"); n != 2 {
		t.Fatalf("count(k) = %d, want 2 (expired entry excluded)", n)
	}
}

// TestSlidingWindow_RecordPurgesExpired 验证 record 会先清理过期时间戳再追加新时间戳。
func TestSlidingWindow_RecordPurgesExpired(t *testing.T) {
	now := time.Now()
	sw := &slidingWindow{counters: make(map[string][]time.Time)}
	sw.counters["k"] = []time.Time{now.Add(-2 * time.Minute)}
	sw.record("k") // 应剔除过期项后追加 now
	if n := sw.count("k"); n != 1 {
		t.Fatalf("count(k) after record = %d, want 1", n)
	}
}

// TestSlidingWindow_Cleanup 验证 cleanup 仅删除全部过期的 key，保留仍有有效时间戳的 key。
func TestSlidingWindow_Cleanup(t *testing.T) {
	now := time.Now()
	sw := &slidingWindow{counters: make(map[string][]time.Time)}
	sw.counters["expired"] = []time.Time{now.Add(-10 * time.Minute)}
	sw.counters["alive"] = []time.Time{now}

	sw.cleanup()

	if _, ok := sw.counters["expired"]; ok {
		t.Fatal("expired key should be deleted by cleanup")
	}
	if _, ok := sw.counters["alive"]; !ok {
		t.Fatal("alive key should be retained by cleanup")
	}
}

// ───────────────────────── 验证码决策引擎 ─────────────────────────

func newTestEngine(emailPerMin, ipPerMin int, ttl time.Duration) *captchaEngine {
	return &captchaEngine{
		cfg: &config.CaptchaConfig{
			Trigger:      config.CaptchaTriggerConfig{EmailPerMinute: emailPerMin, IPPerMinute: ipPerMin},
			WhitelistTTL: ttl,
		},
		emailWindow:   &slidingWindow{counters: make(map[string][]time.Time)},
		ipWindow:      &slidingWindow{counters: make(map[string][]time.Time)},
		whitelist:     make(map[string]captchaWhitelistEntry),
		providerCache: make(map[string]captcha.CaptchaProvider),
	}
}

// TestNeedCaptcha_EmailTrigger 验证 email 维度达到阈值后 needCaptcha 为真。
func TestNeedCaptcha_EmailTrigger(t *testing.T) {
	ce := newTestEngine(3, 0, time.Hour)
	for i := 0; i < 3; i++ {
		ce.recordAttempt("a@x.com", "")
	}
	if !ce.needCaptcha("a@x.com", "") {
		t.Fatal("needCaptcha should be true after 3 email attempts (threshold 3)")
	}
	// 未达到阈值
	ce2 := newTestEngine(3, 0, time.Hour)
	ce2.recordAttempt("a@x.com", "")
	if ce2.needCaptcha("a@x.com", "") {
		t.Fatal("needCaptcha should be false after 1 attempt (threshold 3)")
	}
}

// TestNeedCaptcha_IPTrigger 验证 ip 维度达到阈值后 needCaptcha 为真。
func TestNeedCaptcha_IPTrigger(t *testing.T) {
	ce := newTestEngine(0, 2, time.Hour)
	for i := 0; i < 2; i++ {
		ce.recordAttempt("", "1.2.3.4")
	}
	if !ce.needCaptcha("", "1.2.3.4") {
		t.Fatal("needCaptcha should be true after 2 ip attempts (threshold 2)")
	}
}

// TestNeedCaptcha_DisabledWhenZero 验证阈值为 0 时（配置禁用）不触发验证码。
func TestNeedCaptcha_DisabledWhenZero(t *testing.T) {
	ce := newTestEngine(0, 0, time.Hour)
	for i := 0; i < 100; i++ {
		ce.recordAttempt("spam@x.com", "9.9.9.9")
	}
	if ce.needCaptcha("spam@x.com", "9.9.9.9") {
		t.Fatal("needCaptcha should be false when both thresholds are 0 (disabled)")
	}
}

// TestWhitelist_AddAndIsWhitelisted 验证白名单添加与命中。
func TestWhitelist_AddAndIsWhitelisted(t *testing.T) {
	ce := newTestEngine(1, 0, time.Hour)
	key := "u@x.com|1.2.3.4"
	ce.addWhitelist(key)
	if !ce.isWhitelisted(key) {
		t.Fatal("key should be whitelisted after addWhitelist")
	}
	if ce.isWhitelisted("other|9.9.9.9") {
		t.Fatal("unknown key should not be whitelisted")
	}
}

// TestWhitelist_TTLExpiry 验证白名单 TTL 过期后失效。
func TestWhitelist_TTLExpiry(t *testing.T) {
	// TTL 为负 → 加入即过期
	ce := newTestEngine(1, 0, -time.Hour)
	key := "u@x.com|1.2.3.4"
	ce.addWhitelist(key)
	if ce.isWhitelisted(key) {
		t.Fatal("whitelist entry with negative TTL should already be expired")
	}

	// TTL 为正 → 未过期
	ce2 := newTestEngine(1, 0, time.Hour)
	ce2.addWhitelist(key)
	if !ce2.isWhitelisted(key) {
		t.Fatal("whitelist entry with positive TTL should still be valid")
	}
}

// TestCleanWhitelist 验证过期条目被清理、未过期条目保留。
func TestCleanWhitelist(t *testing.T) {
	ce := newTestEngine(1, 0, time.Hour)
	now := time.Now()
	ce.whitelist["expired"] = captchaWhitelistEntry{expiresAt: now.Add(-time.Hour)}
	ce.whitelist["valid"] = captchaWhitelistEntry{expiresAt: now.Add(time.Hour)}

	ce.cleanWhitelist()

	if _, ok := ce.whitelist["expired"]; ok {
		t.Fatal("expired whitelist entry should be cleaned")
	}
	if _, ok := ce.whitelist["valid"]; !ok {
		t.Fatal("valid whitelist entry should be retained")
	}
}

// ───────────────────────── 请求解析 ─────────────────────────

// TestExtractEmail 验证从请求体提取 email 并做 trim+lowercase 归一化。
func TestExtractEmail(t *testing.T) {
	body, _ := json.Marshal(map[string]interface{}{"email": "Foo@Bar.com"})
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))

	if got := extractEmail(c); got != "foo@bar.com" {
		t.Fatalf("extractEmail = %q, want foo@bar.com", got)
	}

	// 缺失 email
	body2, _ := json.Marshal(map[string]interface{}{"name": "x"})
	c2, _ := gin.CreateTestContext(httptest.NewRecorder())
	c2.Request = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body2))
	if got := extractEmail(c2); got != "" {
		t.Fatalf("extractEmail(missing) = %q, want empty", got)
	}
}

// TestExtractCaptchaToken 验证优先读取 Header，其次从 body 提取，且 body 可被再次读取。
func TestExtractCaptchaToken(t *testing.T) {
	// 1. Header 优先
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{"captcha_token":"from_body"}`)))
	c.Request.Header.Set("X-Captcha-Token", "from_header")
	if got := extractCaptchaToken(c); got != "from_header" {
		t.Fatalf("extractCaptchaToken = %q, want from_header (header priority)", got)
	}
	// Header 命中后不应消费 body：再次读取仍可解析
	raw, err := c.GetRawData()
	if err != nil {
		t.Fatalf("body should be restorable: %v", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("body should still be valid JSON: %v", err)
	}

	// 2. 仅 body
	body, _ := json.Marshal(map[string]interface{}{"captcha_token": "tok123"})
	c2, _ := gin.CreateTestContext(httptest.NewRecorder())
	c2.Request = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	if got := extractCaptchaToken(c2); got != "tok123" {
		t.Fatalf("extractCaptchaToken = %q, want tok123", got)
	}
}

// ───────────────────────── 中间件入口分支 ─────────────────────────

// TestCaptcha_NoneTypePassthrough 验证 captcha.type=none 时中间件直接放行（不触碰引擎）。
func TestCaptcha_NoneTypePassthrough(t *testing.T) {
	mw := Captcha(&config.Config{Captcha: config.CaptchaConfig{Type: "none"}})

	r := gin.New()
	r.Use(mw)
	r.GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ping", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (none type pass-through)", w.Code)
	}
}

// TestStopCaptcha_Idempotent 验证多次调用 StopCaptcha 不 panic（captchaStopOnce 保护，
// 避免关闭已关闭的 channel）。已在 App.shutdown 中与 StopRateLimiter 一并调用。
func TestStopCaptcha_Idempotent(t *testing.T) {
	StopCaptcha()
	StopCaptcha()
}
