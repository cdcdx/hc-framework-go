package middleware

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/metrics"
	"github.com/cdcdx/hc-framework-go/pkg/response"
	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

// tokenBucket 令牌桶封装
type tokenBucket struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// rateLimiter 限流器
type rateLimiter struct {
	mu sync.RWMutex // 保护 global / current / whitelist（热更新重建时独占，热路径读用 RLock 共享）

	global    *tokenBucket            // 全局令牌桶（指针整体替换，读用 RLock）
	current   *config.RateLimitConfig // 当前生效限流配置（指针整体替换，读用 RLock）
	whitelist map[string]bool         // IP / 用户白名单（热更新整体替换，读用 RLock）

	perUser sync.Map // userID -> *tokenBucket（无锁读，仅新建桶时短暂加锁）
	perIP   sync.Map // ip     -> *tokenBucket

	mgr         *config.Manager // 配置管理器（实时读取限流参数，支持热更新）
	lastCfg     *config.Config  // 上一次的配置指针（指针比较判断是否需要重建）
	cleanupTick *time.Ticker
	stopCh      chan struct{} // 关闭信号：StopRateLimiter 中 close，通知后台 cleanup goroutine 退出（避免泄漏）

	exemptPrefixes []string // 豁免限流的路径前缀（热更新整体替换，读用 RLock）
}

var rlInstance *rateLimiter
var rlOnce sync.Once
var rlStopOnce sync.Once

// StopRateLimiter 停止全局限流器的后台 goroutine（cleanup ticker）。
// 应在进程优雅关闭时调用，避免 goroutine 泄漏。幂等，多次调用安全。
func StopRateLimiter() {
	rlStopOnce.Do(func() {
		if rlInstance == nil {
			return
		}
		if rlInstance.cleanupTick != nil {
			rlInstance.cleanupTick.Stop()
		}
		// 关闭 stopCh 唤醒后台 cleanup goroutine（此前仅 Stop ticker，channel 不关闭，
		// `for range cleanupTick.C` 会永久阻塞导致 goroutine 泄漏）。
		if rlInstance.stopCh != nil {
			close(rlInstance.stopCh)
		}
	})
}

func getRateLimiter(mgr *config.Manager) *rateLimiter {
	rlOnce.Do(func() {
		cur := mgr.Get()
		rlc := &cur.RateLimit
		rl := &rateLimiter{
			perUser:   sync.Map{},
			perIP:     sync.Map{},
			whitelist: make(map[string]bool),
			mgr:       mgr,
			current:   rlc,
			lastCfg:   cur,
		}
		for _, ip := range rlc.Whitelist {
			rl.whitelist[ip] = true
		}
		rl.exemptPrefixes = append(rl.exemptPrefixes, rlc.ExemptPaths...)
		if rlc.Global.Enabled {
			rl.global = &tokenBucket{
				limiter:  rate.NewLimiter(rate.Limit(rlc.Global.Rate), rlc.Global.Burst),
				lastSeen: time.Now(),
			}
		}

		// 定期清理过期的用户/IP 令牌桶（10分钟未使用则清理）
		rl.cleanupTick = time.NewTicker(10 * time.Minute)
		rl.stopCh = make(chan struct{})
		go rl.runCleanup(make(chan struct{})) // done 在测试中被用来确认 goroutine 退出；生产径丢弃

		rlInstance = rl
	})
	return rlInstance
}

// runCleanup 后台清理循环：周期性触发 cleanup()，直到 stopCh 被关闭（StopRateLimiter）后退出。
// done 在 goroutine 退出时关闭，用于测试确认「不再泄漏」；生产径传入一次性 channel 丢弃。
// 此前实现为 `for range cleanupTick.C`，但 time.Ticker.Stop 不关闭 channel，导致 Stop 后
// 该 goroutine 永久阻塞（泄漏）—— 改用 select 监听 stopCh 后，Stop 能真正唤醒并退出。
func (rl *rateLimiter) runCleanup(done chan struct{}) {
	defer close(done)
	for {
		select {
		case <-rl.cleanupTick.C:
			rl.cleanup()
		case <-rl.stopCh:
			return
		}
	}
}

// sync 热更新：配置重载且限流参数变化时，重建白名单与全局限流器，
// 已有 per-user/per-IP 桶在 10 分钟无活动后由 cleanup 清理，并以新参数重建。
func (rl *rateLimiter) sync() {
	cur := rl.mgr.Get()
	// 双检查：热路径下先以 RLock 比较指针（近乎零开销、无数据竞争）
	rl.mu.RLock()
	same := rl.lastCfg == cur
	rl.mu.RUnlock()
	if same {
		return
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if rl.lastCfg == cur {
		return
	}
	rlc := &cur.RateLimit
	rl.whitelist = make(map[string]bool)
	for _, ip := range rlc.Whitelist {
		rl.whitelist[ip] = true
	}
	rl.exemptPrefixes = append([]string(nil), rlc.ExemptPaths...)
	if rlc.Global.Enabled {
		rl.global = &tokenBucket{
			limiter:  rate.NewLimiter(rate.Limit(rlc.Global.Rate), rlc.Global.Burst),
			lastSeen: time.Now(),
		}
	} else {
		rl.global = nil
	}
	rl.current = rlc
	rl.lastCfg = cur
}

// isExemptPath 判断请求路径是否命中豁免前缀（心跳等命脉接口）。前缀匹配，读用 RLock 不阻塞热路径。
func (rl *rateLimiter) isExemptPath(path string) bool {
	rl.mu.RLock()
	prefixes := rl.exemptPrefixes
	rl.mu.RUnlock()
	for _, p := range prefixes {
		if p != "" && strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

func (rl *rateLimiter) cleanup() {
	now := time.Now()
	// 后台 goroutine 运行（不受 gin Recovery 兜底），类型断言务必带 ok 守卫：
	// 个别脏 entry 只跳过、绝不 panic 拖垮整个限流后台循环（13 §3.43）。
	rl.perUser.Range(func(k, v interface{}) bool {
		tb, ok := v.(*tokenBucket)
		if !ok {
			return true
		}
		if now.Sub(tb.lastSeen) > 10*time.Minute {
			rl.perUser.Delete(k)
		}
		return true
	})
	rl.perIP.Range(func(k, v interface{}) bool {
		tb, ok := v.(*tokenBucket)
		if !ok {
			return true
		}
		if now.Sub(tb.lastSeen) > 10*time.Minute {
			rl.perIP.Delete(k)
		}
		return true
	})
}

// RateLimit 令牌桶限流中间件（限流参数支持运行时热更新，需求 §9）
func RateLimit(mgr *config.Manager) gin.HandlerFunc {
	rl := getRateLimiter(mgr)

	return func(c *gin.Context) {
		cfg := mgr.Get().RateLimit
		if !cfg.Enabled {
			c.Next()
			return
		}

		// 热更新检测（极少触发，指针比较近乎零开销，内部以 RWMutex 保护）
		rl.sync()

		clientIP := c.ClientIP()
		userID := c.GetString("user_id")

		// 白名单检查（受 RWMutex 保护，热路径以 RLock 读取，不阻塞其它请求）
		rl.mu.RLock()
		whitelisted := rl.whitelist[clientIP] || (userID != "" && rl.whitelist[userID])
		rl.mu.RUnlock()
		if whitelisted {
			c.Next()
			return
		}

		// 豁免路径（如心跳）：命脉接口不被全局/用户/IP 配额限制，避免限流 429 误判设备离线。
		// 仍统计指标便于观测实际流量。
		if rl.isExemptPath(c.Request.URL.Path) {
			metrics.RateLimitTotal.WithLabelValues("exempt", "allowed").Inc()
			c.Next()
			return
		}

		// 全局限流
		rl.mu.RLock()
		global := rl.global
		rl.mu.RUnlock()
		if global != nil && !global.limiter.Allow() {
			metrics.RateLimitTotal.WithLabelValues("global", "rejected").Inc()
			setRateLimitHeaders(c, global)
			response.RateLimited(c, 1)
			c.Abort()
			return
		}

		// 用户级别限流
		rl.mu.RLock()
		perUserEnabled := rl.current.PerUser.Enabled
		rl.mu.RUnlock()
		if perUserEnabled && userID != "" {
			if !rl.checkPerUser(userID) {
				metrics.RateLimitTotal.WithLabelValues("per_user", "rejected").Inc()
				setRateLimitHeaders(c, rl.bucketFor("user", userID))
				response.RateLimited(c, 1)
				c.Abort()
				return
			}
		}

		// IP 级别限流
		rl.mu.RLock()
		perIPEnabled := rl.current.PerIP.Enabled
		rl.mu.RUnlock()
		if perIPEnabled {
			if !rl.checkPerIP(clientIP) {
				metrics.RateLimitTotal.WithLabelValues("per_ip", "rejected").Inc()
				setRateLimitHeaders(c, rl.bucketFor("ip", clientIP))
				response.RateLimited(c, 1)
				c.Abort()
				return
			}
		}

		// 成功路径：透出当前限流状态，便于客户端感知配额与退避
		tb, bucketType := rl.activeBucket(userID, clientIP)
		if tb != nil {
			setRateLimitHeaders(c, tb)
			metrics.RateLimitTotal.WithLabelValues(bucketType, "allowed").Inc()
		}

		c.Next()
	}
}

// activeBucket 返回请求实际受限的桶（成功路径透出响应头用）：
// 优先 per-user，其次 per-IP，再次 global；同时返回桶类型名供指标使用。
func (rl *rateLimiter) activeBucket(userID, clientIP string) (*tokenBucket, string) {
	rl.mu.RLock()
	cfg := rl.current
	rl.mu.RUnlock()
	if cfg.PerUser.Enabled && userID != "" {
		if v, ok := rl.perUser.Load(userID); ok {
			return v.(*tokenBucket), "per_user"
		}
	}
	if cfg.PerIP.Enabled {
		if v, ok := rl.perIP.Load(clientIP); ok {
			return v.(*tokenBucket), "per_ip"
		}
	}
	rl.mu.RLock()
	global := rl.global
	rl.mu.RUnlock()
	return global, "global"
}

// bucketFor 返回指定用户/IP 的令牌桶（用于被限流时透出响应头），不存在时返回 nil。
func (rl *rateLimiter) bucketFor(kind, key string) *tokenBucket {
	if kind == "user" {
		if v, ok := rl.perUser.Load(key); ok {
			return v.(*tokenBucket)
		}
	} else {
		if v, ok := rl.perIP.Load(key); ok {
			return v.(*tokenBucket)
		}
	}
	return nil
}

// checkPerKey 对单个 sync.Map 按 key 做令牌桶限流（per-user / per-IP 共用）。
// 已存在的桶走无锁 Load；新建桶时用传入的 rate/burst 创建（调用方负责从当前配置安全读取）。
func (rl *rateLimiter) checkPerKey(m *sync.Map, key string, r int, burst int) bool {
	if v, ok := m.Load(key); ok {
		tb := v.(*tokenBucket)
		tb.lastSeen = time.Now()
		return tb.limiter.Allow()
	}
	tb := &tokenBucket{
		limiter:  rate.NewLimiter(rate.Limit(float64(r)), burst),
		lastSeen: time.Now(),
	}
	actual, loaded := m.LoadOrStore(key, tb)
	if loaded {
		tb = actual.(*tokenBucket)
	}
	tb.lastSeen = time.Now()
	return tb.limiter.Allow()
}

func (rl *rateLimiter) checkPerUser(userID string) bool {
	rl.mu.RLock()
	cfg := rl.current
	rl.mu.RUnlock()
	return rl.checkPerKey(&rl.perUser, userID, cfg.PerUser.Rate, cfg.PerUser.Burst)
}

func (rl *rateLimiter) checkPerIP(ip string) bool {
	rl.mu.RLock()
	cfg := rl.current
	rl.mu.RUnlock()
	return rl.checkPerKey(&rl.perIP, ip, cfg.PerIP.Rate, cfg.PerIP.Burst)
}

func setRateLimitHeaders(c *gin.Context, tb *tokenBucket) {
	if tb == nil {
		return
	}
	limit := int(tb.limiter.Limit())
	tokens := int(tb.limiter.Tokens())
	// Reset：补充一个令牌所需的时长（秒级 Unix 时间戳），比固定的 +1s 更准确
	var reset int64
	if r := tb.limiter.Limit(); r > 0 {
		reset = time.Now().Add(time.Duration(float64(time.Second) / float64(r))).Unix()
	} else {
		reset = time.Now().Add(time.Second).Unix()
	}
	c.Header("X-RateLimit-Limit", fmt.Sprintf("%d", limit))
	c.Header("X-RateLimit-Remaining", fmt.Sprintf("%d", tokens))
	c.Header("X-RateLimit-Reset", fmt.Sprintf("%d", reset))
}
