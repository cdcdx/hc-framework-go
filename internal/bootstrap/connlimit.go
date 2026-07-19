package bootstrap

import (
	"net"
	"sync"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// trackedConn 包装 net.Conn，在 Close 时回调以释放连接计数（全局/单 IP 信号量）。
type trackedConn struct {
	net.Conn
	onClose func()
	once    sync.Once
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.onClose)
	return err
}

// connLimitListener 在 Accept 阶段实施连接数上限（高并发可用性加固，需求 §6.4）：
//   - 全局并发连接数上限（cfg.MaxConns）：超过上限的新连接被立即关闭（连接级快速失败），
//     保护后端资源不被连接风暴打垮；
//   - 单 IP 并发连接数上限（cfg.MaxConnsPerIP）：防止单一客户端耗尽连接，提供基础连接级限流。
//
// 为何放在 Listener 而非 http.Server：Go 标准库 http.Server 没有 MaxConns/MaxConnsPerIP
// 字段，只能由 Listener 在握手前于 Accept 阶段拦截。超限连接直接 Close，不进入 HTTP 栈，
// 因此也不占用 gin 中间件/worker 资源（K8s 下仍依赖 readiness/HPA 做进程级容量兜底）。
type connLimitListener struct {
	net.Listener
	cfg      config.ServerConfig // 仅读取 MaxConns / MaxConnsPerIP，按值传入即可
	globalCh chan struct{}       // 全局并发连接信号量；nil 表示不限
	mu       sync.Mutex
	perIP    map[string]int // 各 IP 当前并发连接数
}

// newConnLimitListener 包装底层 Listener，按配置启用连接级限流（0 表示不限）。
func newConnLimitListener(l net.Listener, cfg config.ServerConfig) *connLimitListener {
	cl := &connLimitListener{
		Listener: l,
		cfg:      cfg,
		perIP:    make(map[string]int),
	}
	if cfg.MaxConns > 0 {
		cl.globalCh = make(chan struct{}, cfg.MaxConns)
	}
	return cl
}

// Accept 循环：获取连接后判定是否超限，超限则立即关闭并继续 Accept，否则包成 trackedConn。
// 设计为「超限即丢」而非阻塞等待，避免 SYN 队列被慢连接占满，符合快速失败（fail-fast）原则。
func (cl *connLimitListener) Accept() (net.Conn, error) {
	for {
		conn, err := cl.Listener.Accept()
		if err != nil {
			return nil, err
		}

		// 全局并发连接数上限
		if cl.globalCh != nil {
			select {
			case cl.globalCh <- struct{}{}:
			default:
				_ = conn.Close()
				continue
			}
		}

		// 单 IP 并发连接数上限
		if cl.cfg.MaxConnsPerIP > 0 {
			ip := clientIP(conn.RemoteAddr())
			cl.mu.Lock()
			if cl.perIP[ip] >= cl.cfg.MaxConnsPerIP {
				cl.mu.Unlock()
				_ = conn.Close()
				if cl.globalCh != nil {
					<-cl.globalCh
				}
				continue
			}
			cl.perIP[ip]++
			cl.mu.Unlock()

			return &trackedConn{
				Conn: conn,
				onClose: func() {
					cl.mu.Lock()
					cl.perIP[ip]--
					if cl.perIP[ip] <= 0 {
						delete(cl.perIP, ip)
					}
					cl.mu.Unlock()
					if cl.globalCh != nil {
						<-cl.globalCh
					}
				},
			}, nil
		}

		// 仅全局限制（无单 IP 限制）：直接返回带全局计数释放的包装连接
		return &trackedConn{
			Conn: conn,
			onClose: func() {
				if cl.globalCh != nil {
					<-cl.globalCh
				}
			},
		}, nil
	}
}

// clientIP 从 RemoteAddr（"host:port"）提取主机地址，用于单 IP 连接计数。
// 经过反向代理时取到的是代理 IP（best-effort），真实客户端 IP 需依赖 Proxy 协议/转发头，
// 此处仅做连接级粗粒度限流，语义足够。
func clientIP(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}
