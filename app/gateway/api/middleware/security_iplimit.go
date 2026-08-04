package middleware

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/svc"
	"github.com/cdcdx/hc-framework-go/common/response"
)

// SecurityIPLimitMiddleware 按客户端 IP 的固定窗口限频（进程内实现）。
// 单实例有效；多实例部署需换 Redis 版（go-zero rest.WithLimiters / periodlimit）。
type SecurityIPLimitMiddleware struct {
	svcCtx *svc.ServiceContext

	mu      sync.Mutex
	buckets map[string]*ipBucket
}

type ipBucket struct {
	windowStart time.Time
	count       int
}

// NewSecurityIPLimitMiddleware 创建 IP 限频中间件，并启动后台清理协程。
func NewSecurityIPLimitMiddleware(svcCtx *svc.ServiceContext) *SecurityIPLimitMiddleware {
	m := &SecurityIPLimitMiddleware{
		svcCtx:  svcCtx,
		buckets: make(map[string]*ipBucket),
	}
	// 每 5 分钟清理超过 2 分钟未更新的 IP 桶，防止内存泄漏
	go m.cleanupLoop()
	return m
}

// cleanupLoop 定期清理过期 bucket，避免不再活跃的 IP 永久占用内存。
func (m *SecurityIPLimitMiddleware) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		m.mu.Lock()
		cutoff := time.Now().Add(-2 * time.Minute)
		for ip, b := range m.buckets {
			if b.windowStart.Before(cutoff) {
				delete(m.buckets, ip)
			}
		}
		m.mu.Unlock()
	}
}

// Handle 中间件处理
func (m *SecurityIPLimitMiddleware) Handle(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !m.svcCtx.Config.Security.IPLimitEnabled {
			next(w, r)
			return
		}

		limit := m.svcCtx.Config.Security.IPLimitPerMinute
		if limit <= 0 {
			limit = 30
		}
		ip := clientIP(r)
		now := time.Now()

		m.mu.Lock()
		b, ok := m.buckets[ip]
		if !ok || now.Sub(b.windowStart) >= time.Minute {
			b = &ipBucket{windowStart: now}
			m.buckets[ip] = b
		}
		b.count++
		over := b.count > limit
		m.mu.Unlock()

		if over {
			response.RateLimited(w, r, 1)
			return
		}
		next(w, r)
	}
}

// clientIP 提取客户端 IP
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if ip := strings.TrimSpace(parts[0]); ip != "" {
			return ip
		}
	}
	if xr := r.Header.Get("X-Real-IP"); xr != "" {
		return xr
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
