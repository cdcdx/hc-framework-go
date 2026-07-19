package middleware

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/degrade"
	"github.com/cdcdx/hc-framework-go/internal/metrics"
	"github.com/cdcdx/hc-framework-go/pkg/logger"
	"github.com/cdcdx/hc-framework-go/pkg/response"
	"github.com/gin-gonic/gin"
	"github.com/sony/gobreaker/v2"
	"go.uber.org/zap"
)

// cbManager 熔断器管理器
type cbManager struct {
	mu       sync.RWMutex
	breakers map[string]*gobreaker.CircuitBreaker[any]
	mgr      *config.Manager              // 配置管理器（实时读取熔断阈值，支持热更新）
	current  *config.CircuitBreakerConfig // 当前生效配置快照
	lastCfg  *config.Config               // 上一次配置指针（用于热更新检测）
}

var cbInstance *cbManager
var cbOnce sync.Once

func getCBManager(mgr *config.Manager) *cbManager {
	cbOnce.Do(func() {
		cur := mgr.Get()
		cbInstance = &cbManager{
			breakers: make(map[string]*gobreaker.CircuitBreaker[any]),
			mgr:      mgr,
			current:  &cur.CircuitBreaker,
			lastCfg:  cur,
		}
	})
	return cbInstance
}

// sync 热更新：配置重载且熔断参数变化时，清空已创建的熔断器（下一请求按新阈值重建）。
func (m *cbManager) sync() {
	cur := m.mgr.Get()
	if m.lastCfg == cur {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lastCfg == cur {
		return
	}
	m.breakers = make(map[string]*gobreaker.CircuitBreaker[any])
	m.current = &cur.CircuitBreaker
	m.lastCfg = cur
}

func (m *cbManager) getOrCreate(route string) *gobreaker.CircuitBreaker[any] {
	m.sync()

	m.mu.RLock()
	cb, ok := m.breakers[route]
	m.mu.RUnlock()
	if ok {
		return cb
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 双重检查
	if cb, ok = m.breakers[route]; ok {
		return cb
	}

	cfg := m.current
	settings := gobreaker.Settings{
		Name:        fmt.Sprintf("cb-%s", route),
		MaxRequests: uint32(cfg.HalfOpenMaxRequests),
		Interval:    cfg.Interval,
		Timeout:     cfg.Timeout,

		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 5 && failureRatio >= cfg.FailureThreshold
		},

		OnStateChange: func(name string, from, to gobreaker.State) {
			logger.L().Info("circuit breaker state changed",
				zap.String("name", name),
				zap.String("from", from.String()),
				zap.String("to", to.String()),
			)
			// 熔断器打开 → 强制全局降级（读写均降级，需求 §11）；
			// 熔断器恢复（CLOSED）→ 解除降级（当无其它打开的熔断器时）。
			var stateVal float64
			switch to {
			case gobreaker.StateOpen:
				degrade.OnBreakerOpen()
				stateVal = 2
			case gobreaker.StateHalfOpen:
				stateVal = 1
			case gobreaker.StateClosed:
				degrade.OnBreakerClosed()
				stateVal = 0
			}
			metrics.CircuitBreakerState.WithLabelValues(route).Set(stateVal)
			metrics.CircuitBreakerTransitionsTotal.WithLabelValues(route, from.String(), to.String()).Inc()
		},

		IsSuccessful: func(err error) bool {
			return err == nil
		},
	}

	cb = gobreaker.NewCircuitBreaker[any](settings)
	m.breakers[route] = cb

	return cb
}

// CircuitBreaker 三态熔断器中间件（熔断阈值支持运行时热更新，需求 §9）
func CircuitBreaker(mgr *config.Manager) gin.HandlerFunc {
	m := getCBManager(mgr)

	return func(c *gin.Context) {
		if !mgr.Get().CircuitBreaker.Enabled {
			c.Next()
			return
		}

		// 按路由分组独立熔断
		route := c.FullPath()
		if route == "" {
			route = c.Request.URL.Path
		}

		cb := m.getOrCreate(route)

		// 检查熔断器状态
		if cb.State() == gobreaker.StateOpen {
			response.ErrorWithHTTPStatus(c, http.StatusServiceUnavailable, 10602,
				fmt.Sprintf("circuit breaker open for %s", route))
			c.Abort()
			return
		}

		// 执行业务逻辑
		_, err := cb.Execute(func() (any, error) {
			c.Next()

			// 检查业务处理结果
			if c.Writer.Status() >= 500 {
				return nil, fmt.Errorf("server error: %d", c.Writer.Status())
			}
			return nil, nil
		})

		if err != nil {
			// 熔断器打开或执行失败
			if !c.IsAborted() {
				response.ErrorWithHTTPStatus(c, http.StatusServiceUnavailable, 10602,
					"service temporarily unavailable")
				c.Abort()
			}
		}
	}
}
