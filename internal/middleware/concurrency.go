package middleware

import (
	"sync"
	"sync/atomic"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/metrics"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/pkg/response"
	"github.com/gin-gonic/gin"
	"golang.org/x/sync/semaphore"
)

// ConcurrencyLimit 应用层有界并发限流（负载卸载 / load shedding，高并发高可用加固，见文档 20）。
//
// 防护分层的定位：在「连接级上限(MaxConns)」与「令牌桶限流(RateLimit)」之后、业务处理器之前，
// 对「进入业务层、占用 DB/缓存/下游连接」的在途请求数再设一道进程级硬上限。达到上限即非阻塞
// 快速失败返回 503（Service Unavailable），而不是让请求在应用层无限堆积、逐渐拖垮后端连接池与
// 下游服务——这是典型的高并发过载保护（load shedding）手段，与限流（拒绝「超速率」请求）、
// 熔断（拒绝「故障下游」请求）互补：限流按「速率」、并发限流按「同时进行的量」。
//
// 与限流/熔断的差别：
//   - 限流(RateLimit)：单位时间允许多少个请求（令牌桶），超出返回 429。
//   - 并发限流(本中间件)：同一时刻最多有多少个请求在「被处理」，超出返回 503。
//     二者维度不同，组合使用可同时约束「速率」与「在途量」，避免短时突发（速率未超但瞬时并发极高）
//     把 DB 连接池打满。
//
// 实现要点：
//   - 用 golang.org/x/sync/semaphore.Weighted 做有界信号量；TryAcquire 非阻塞，拿不到立即 503，
//     不引入额外排队等待（排队会放大尾延迟、与 RequestTimeout 抢夺资源）。
//   - 系统/运维端点（/health、/ready、/metrics、/debug/*、/swagger）豁免：K8s 探针与指标抓取、
//     运维 API 在过载时仍须可用，否则就绪探针返回 503 会触发 K8s 误杀正在排空的 Pod。
//   - 容量在启动期固定（信号量不可动态伸缩），故与 MaxConns 一致为「重启生效」，不受配置热更新影响；
//     该上限是容量规划结果（≈ DB 连接池上限 / 单请求平均下游并发度），而非「越大越好」。
//   - in-flight 计数经 Prometheus gauge 暴露，便于观测并发水位与配置合理性。
func ConcurrencyLimit(mgr *config.Manager) gin.HandlerFunc {
	cl := getConcurrencyLimiter(mgr)
	return func(c *gin.Context) {
		// 系统/运维端点豁免：K8s 探针与指标抓取不能被应用层并发上限拦截。
		if isConcurrencyExempt(c.Request.URL.Path) {
			c.Next()
			return
		}
		// 未启用（limit<=0）：直通，不影响既有部署语义。
		if cl.sem == nil {
			c.Next()
			return
		}
		// 非阻塞获取一个并发配额：拿不到即负载卸载（快速失败），避免请求堆积压垮后端。
		if !cl.sem.TryAcquire(1) {
			metrics.HTTPConcurrencyRejectedTotal.Inc()
			response.ServiceUnavailable(c, model.CodeServiceUnavailable)
			c.Abort()
			return
		}
		n := atomic.AddInt64(&cl.inflight, 1)
		metrics.HTTPConcurrencyInFlight.Set(float64(n))
		defer func() {
			atomic.AddInt64(&cl.inflight, -1)
			cl.sem.Release(1)
			metrics.HTTPConcurrencyInFlight.Set(float64(atomic.LoadInt64(&cl.inflight)))
		}()
		c.Next()
	}
}

// concurrencyLimiter 进程级有界并发信号量（启动期固定容量，重启生效）。
type concurrencyLimiter struct {
	sem      *semaphore.Weighted // nil 表示未启用（limit<=0）
	inflight int64               // 当前在途请求数（原子读写，供 gauge 暴露）
}

var clInstance *concurrencyLimiter
var clOnce sync.Once

// getConcurrencyLimiter 返回进程级单例并发限流器（按当前配置在首次调用时固定容量）。
func getConcurrencyLimiter(mgr *config.Manager) *concurrencyLimiter {
	clOnce.Do(func() {
		limit := int64(mgr.Get().Server.ConcurrencyLimit)
		cl := &concurrencyLimiter{}
		if limit > 0 {
			cl.sem = semaphore.NewWeighted(limit)
		}
		clInstance = cl
	})
	return clInstance
}

// isConcurrencyExempt 判断路径是否豁免应用层并发上限（系统/运维端点）。
func isConcurrencyExempt(path string) bool {
	switch path {
	case "/health", "/ready":
		return true
	}
	switch {
	case len(path) >= 8 && path[:8] == "/metrics": // /metrics、/metrics/cache
		return true
	case len(path) >= 7 && path[:7] == "/debug/": // 运维 API（reload/loglevel）
		return true
	case len(path) >= 9 && path[:9] == "/swagger": // 文档
		return true
	}
	return false
}
