package middleware

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/pkg/captcha"
	"github.com/cdcdx/hc-framework-go/pkg/response"
	"github.com/gin-gonic/gin"
)

// captchaWhitelistEntry 白名单条目
type captchaWhitelistEntry struct {
	expiresAt time.Time
}

// slidingWindow 滑动窗口计数器
type slidingWindow struct {
	mu       sync.Mutex
	counters map[string][]time.Time
}

func newSlidingWindow() *slidingWindow {
	sw := &slidingWindow{counters: make(map[string][]time.Time)}
	// 定期清理过期数据（每2分钟）。select 监听 captchaStopCh：StopCaptcha 关闭后退出，
	// 释放 ticker 资源（此前 for range tick.C 在进程关闭时永久阻塞、ticker 不释放）。
	go func() {
		tick := time.NewTicker(2 * time.Minute)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				sw.cleanup()
			case <-captchaStopCh:
				return
			}
		}
	}()
	return sw
}

func (sw *slidingWindow) record(key string) int {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-1 * time.Minute)

	ts := sw.counters[key]
	// 清理过期
	i := 0
	for i < len(ts) && ts[i].Before(cutoff) {
		i++
	}
	ts = ts[i:]
	ts = append(ts, now)
	sw.counters[key] = ts
	return len(ts)
}

func (sw *slidingWindow) count(key string) int {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	cutoff := time.Now().Add(-1 * time.Minute)
	ts := sw.counters[key]
	n := 0
	for _, t := range ts {
		if t.After(cutoff) {
			n++
		}
	}
	return n
}

func (sw *slidingWindow) cleanup() {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	cutoff := time.Now().Add(-5 * time.Minute)
	for k, ts := range sw.counters {
		i := 0
		for i < len(ts) && ts[i].Before(cutoff) {
			i++
		}
		if i == len(ts) {
			delete(sw.counters, k)
		} else {
			sw.counters[k] = ts[i:]
		}
	}
}

// captchaEngine 验证码决策引擎
type captchaEngine struct {
	mu            sync.RWMutex
	provider      captcha.CaptchaProvider
	cfg           *config.CaptchaConfig
	emailWindow   *slidingWindow
	ipWindow      *slidingWindow
	whitelist     map[string]captchaWhitelistEntry // key: "email|ip"
	providerCache map[string]captcha.CaptchaProvider
}

var ceInstance *captchaEngine
var ceOnce sync.Once

// captchaStopCh 关闭信号：StopCaptcha 中 close，通知验证码后台清理 goroutine（滑动窗口清理、
// 白名单清理）退出。这些 goroutine 随全局单例创建、进程生命周期内常驻，故需显式停止以释放 ticker 资源。
var captchaStopCh = make(chan struct{})
var captchaStopOnce sync.Once

// StopCaptcha 关闭验证码后台清理 goroutine，释放 ticker 资源（避免进程关闭时泄漏）。
// 在 App.shutdown 中与 StopRateLimiter 一并调用；幂等安全（重复 close 由 captchaStopOnce 保护）。
func StopCaptcha() {
	captchaStopOnce.Do(func() { close(captchaStopCh) })
}

func getCaptchaEngine(cfg *config.Config) *captchaEngine {
	ceOnce.Do(func() {
		ce := &captchaEngine{
			cfg:           &cfg.Captcha,
			emailWindow:   newSlidingWindow(),
			ipWindow:      newSlidingWindow(),
			whitelist:     make(map[string]captchaWhitelistEntry),
			providerCache: make(map[string]captcha.CaptchaProvider),
		}

		// 初始化对应类型的验证器
		switch cfg.Captcha.Type {
		case "tencent":
			ce.provider = captcha.NewTencentVerifier(
				cfg.Captcha.Tencent.AppID,
				cfg.Captcha.Tencent.SecretKey,
			)
		case "turnstile":
			ce.provider = captcha.NewTurnstileVerifier(
				cfg.Captcha.Turnstile.SiteKey,
				cfg.Captcha.Turnstile.SecretKey,
			)
		case "recaptcha":
			ce.provider = captcha.NewReCAPTCHAVerifier(
				cfg.Captcha.ReCAPTCHA.SiteKey,
				cfg.Captcha.ReCAPTCHA.SecretKey,
			)
		case "hcaptcha":
			ce.provider = captcha.NewhCAPTCHAVerifier(
				cfg.Captcha.HCAPTCHA.SiteKey,
				cfg.Captcha.HCAPTCHA.SecretKey,
			)
		}

		// 定期清理过期白名单。监听 captchaStopCh：StopCaptcha 关闭后退出，释放 ticker 资源。
		go func() {
			tick := time.NewTicker(1 * time.Minute)
			defer tick.Stop()
			for {
				select {
				case <-tick.C:
					ce.cleanWhitelist()
				case <-captchaStopCh:
					return
				}
			}
		}()

		ceInstance = ce
	})
	return ceInstance
}

func (ce *captchaEngine) cleanWhitelist() {
	ce.mu.Lock()
	defer ce.mu.Unlock()

	now := time.Now()
	for k, v := range ce.whitelist {
		if now.After(v.expiresAt) {
			delete(ce.whitelist, k)
		}
	}
}

// isWhitelisted 检查是否在白名单中
func (ce *captchaEngine) isWhitelisted(key string) bool {
	ce.mu.RLock()
	defer ce.mu.RUnlock()

	entry, ok := ce.whitelist[key]
	if !ok {
		return false
	}
	if time.Now().After(entry.expiresAt) {
		return false
	}
	return true
}

// addWhitelist 添加到白名单
func (ce *captchaEngine) addWhitelist(key string) {
	ce.mu.Lock()
	defer ce.mu.Unlock()

	ce.whitelist[key] = captchaWhitelistEntry{
		expiresAt: time.Now().Add(ce.cfg.WhitelistTTL),
	}
}

// needCaptcha 判断是否需要验证码
// 触发规则：email / ip 任一维度计数达到对应阈值即触发。
// 阈值为 0 表示禁用该维度（计数守卫已用 >0 跳过统计，此处 return 也必须按 >0 判定，
// 否则 ipCount(0) >= IPPerMinute(0) 恒为 true，导致配置禁用维度后反而每次请求都要求验证码）。
func (ce *captchaEngine) needCaptcha(email, ip string) bool {
	triggered := false

	if email != "" && ce.cfg.Trigger.EmailPerMinute > 0 {
		triggered = triggered || ce.emailWindow.count(email) >= ce.cfg.Trigger.EmailPerMinute
	}

	if ip != "" && ce.cfg.Trigger.IPPerMinute > 0 {
		triggered = triggered || ce.ipWindow.count(ip) >= ce.cfg.Trigger.IPPerMinute
	}

	return triggered
}

// recordAttempt 记录本次请求
func (ce *captchaEngine) recordAttempt(email, ip string) {
	if email != "" {
		ce.emailWindow.record(email)
	}
	if ip != "" {
		ce.ipWindow.record(ip)
	}
}

// extractRequestJSONField 从请求 Body 中提取指定 JSON 字段（可重入读取，Body 被还原）。
// 返回空字符串表示字段不存在或解析失败。
func extractRequestJSONField(c *gin.Context, field string) string {
	raw, err := c.GetRawData()
	if err != nil {
		return ""
	}
	c.Request.Body = io.NopCloser(bytes.NewBuffer(raw))

	var body map[string]interface{}
	if err := json.Unmarshal(raw, &body); err != nil {
		return ""
	}
	if v, ok := body[field].(string); ok {
		return v
	}
	return ""
}

// extractCaptchaToken 从请求中提取验证码 token
func extractCaptchaToken(c *gin.Context) string {
	// 1. 优先从 Header 获取
	if token := c.GetHeader("X-Captcha-Token"); token != "" {
		return token
	}
	// 2. 尝试从请求体中提取 captcha_token 字段
	return extractRequestJSONField(c, "captcha_token")
}

// extractEmail 从请求体中提取邮箱
func extractEmail(c *gin.Context) string {
	email := extractRequestJSONField(c, "email")
	if email == "" {
		return ""
	}
	return strings.TrimSpace(strings.ToLower(email))
}

// Token 返回当前 provider 的前端脚本地址（供 /api/v1/captcha/script 使用）
func (ce *captchaEngine) ScriptSrc() string {
	if ce.provider != nil {
		return ce.provider.GetScriptSrc()
	}
	return ""
}

// Captcha 防水墙中间件
// 流程：
//  1. captcha.type="none" → 放行
//  2. 提取 email / IP，记录到滑动窗口
//  3. 检查是否触发阈值 → 未触发则放行，已记录计数
//  4. 触发 → 检查白名单 (email+IP)→ 命中白名单则放行
//  5. 未命中白名单 → 检查 X-Captcha-Token / captcha_token
//  6. 无 token → 返回 10205 "需要验证码"
//  7. 验证 token → 失败返回 10603 "需通过验证码"
//  8. 验证成功 → 加入白名单 (TTL=whitelist_ttl) → 放行
func Captcha(cfg *config.Config) gin.HandlerFunc {
	if cfg.Captcha.Type == "none" || cfg.Captcha.Type == "" {
		return func(c *gin.Context) {
			c.Next()
		}
	}

	engine := getCaptchaEngine(cfg)

	return func(c *gin.Context) {
		clientIP := c.ClientIP()
		email := extractEmail(c)

		// 记录本次请求
		engine.recordAttempt(email, clientIP)

		// 检查是否触发验证码阈值
		if !engine.needCaptcha(email, clientIP) {
			c.Next()
			return
		}

		// 已触发阈值，检查白名单
		whitelistKey := email + "|" + clientIP
		if engine.isWhitelisted(whitelistKey) {
			c.Next()
			return
		}

		// 检查是否有 captcha token
		token := extractCaptchaToken(c)
		if token == "" {
			response.Error(c, model.CodeCaptchaRequired)
			c.Abort()
			return
		}

		// 验证 token
		ok, err := engine.provider.Verify(token, clientIP)
		if err != nil || !ok {
			errMsg := model.Message(model.CodeCaptchaVerify)
			if err != nil {
				errMsg = err.Error()
			}
			response.Error(c, model.CodeCaptchaVerify, errMsg)
			c.Abort()
			return
		}

		// 验证通过，加入白名单
		engine.addWhitelist(whitelistKey)
		c.Next()
	}
}
