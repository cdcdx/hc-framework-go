package middleware

import (
	"sync"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/pkg/response"
	"github.com/gin-gonic/gin"
)

// securityWindows 按类型（register/login）隔离的滑动窗口限流器
var securityWindows = map[string]*slidingWindow{}
var securityWindowsMu sync.Mutex

// securityWindow 获取（或创建）指定类型的滑动窗口
func securityWindow(kind string) *slidingWindow {
	securityWindowsMu.Lock()
	defer securityWindowsMu.Unlock()
	if sw, ok := securityWindows[kind]; ok {
		return sw
	}
	sw := newSlidingWindow()
	securityWindows[kind] = sw
	return sw
}

// SecurityIPLimit 基于 IP 的安全限流中间件，消费 security.ip_limit 配置。
// kind 取值 "register" / "login"，分别对应 register_per_minute / login_per_minute。
// 当某 IP 在 1 分钟内请求数达到阈值，直接返回 429（CodeRateLimited）。
// 限流阈值配置为 <=0 时该中间件退化为空操作（不限制）。
// 命中 ratelimit.whitelist（IP）的请求豁免安全限流，便于压测放行 127.0.0.1 等，
// 同时保留生产环境防护（与全局 RateLimit 中间件共用同一白名单）。
// 阈值支持运行时热更新（需求 §9）：每次请求实时读取最新配置。
func SecurityIPLimit(mgr *config.Manager, kind string) gin.HandlerFunc {
	sw := securityWindow(kind)

	return func(c *gin.Context) {
		// 白名单豁免（与全局限流 whitelist 一致）：压测环境将 127.0.0.1 加入即可放行，
		// 不削弱生产防护。
		if securityWhitelisted(c.ClientIP(), mgr) {
			c.Next()
			return
		}

		ipLimit := mgr.Get().Security.IPLimit
		var limit int
		switch kind {
		case "register":
			limit = ipLimit.RegisterPerMinute
		case "login":
			limit = ipLimit.LoginPerMinute
		default:
			limit = 0
		}

		if limit <= 0 {
			c.Next()
			return
		}

		// 先判断是否已超限（已超限则拦截，不计入本次）
		if sw.count(c.ClientIP()) >= limit {
			response.RateLimited(c, 1)
			c.Abort()
			return
		}
		sw.record(c.ClientIP())
		c.Next()
	}
}

// securityWhitelisted 判断给定 IP 是否命中 ratelimit.whitelist（与全局限流白名单共用同一配置）。
// 命中则跳过安全限流，常用于压测环境放行 127.0.0.1。
func securityWhitelisted(clientIP string, mgr *config.Manager) bool {
	for _, ip := range mgr.Get().RateLimit.Whitelist {
		if ip == clientIP {
			return true
		}
	}
	return false
}
