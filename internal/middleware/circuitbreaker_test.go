package middleware

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/cdcdx/hc-framework-go/internal/metrics"
)

// resetCircuitBreaker 重置熔断单例，保证每个测试用例使用自己的配置。
func resetCircuitBreaker() {
	cbOnce = sync.Once{}
	cbInstance = nil
}

const cbEnabledConfig = `
server:
  port: 8080
  mode: test
metrics:
  enabled: true
circuit_breaker:
  enabled: true
  failure_threshold: 0.5
  interval: 0s
  timeout: 2s
  half_open_max_requests: 1
`

// TestCircuitBreakerMiddleware_OpensAfterFailures 验证持续 500 后熔断器打开，
// 后续请求直接返回 503，且 circuit_breaker_state{route} 置为 2（open）。
func TestCircuitBreakerMiddleware_OpensAfterFailures(t *testing.T) {
	resetCircuitBreaker()
	mgr := loadTestManager(t, cbEnabledConfig)

	r := gin.New()
	r.Use(CircuitBreaker(mgr))
	r.GET("/api/v1/flaky", func(c *gin.Context) { c.Status(http.StatusInternalServerError) })

	sawOpen := false
	// 持续发送失败请求：熔断需累计 >=5 次失败才会打开；打开后请求直接返回 503。
	for i := 0; i < 10; i++ {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/flaky", nil))
		if w.Code == http.StatusServiceUnavailable {
			sawOpen = true
		}
	}
	if !sawOpen {
		t.Fatal("circuit breaker never returned 503 after sustained 500s")
	}

	if state := testutil.ToFloat64(metrics.CircuitBreakerState.WithLabelValues("/api/v1/flaky")); state != 2 {
		t.Errorf("circuit_breaker_state{/api/v1/flaky} = %v, want 2 (open)", state)
	}
}
