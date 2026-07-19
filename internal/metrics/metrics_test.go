package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cdcdx/hc-framework-go/internal/cache"
)

var fqNameRe = regexp.MustCompile(`fqName: "([^"]+)"`)

func metricName(m prometheus.Metric) string {
	if mm := fqNameRe.FindStringSubmatch(m.Desc().String()); len(mm) == 2 {
		return mm[1]
	}
	return m.Desc().String()
}

func TestPrometheusExposition(t *testing.T) {
	// 模拟一次请求与一次限流拒绝，验证埋点进入输出
	HTTPRequestsTotal.WithLabelValues("GET", "/api/v1/idle/start", "200").Inc()
	HTTPRequestDuration.WithLabelValues("GET", "/api/v1/idle/start").Observe(0.01)
	RateLimitTotal.WithLabelValues("global", "rejected").Inc()
	CircuitBreakerState.WithLabelValues("/api/v1/shop/redeem").Set(2)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("expected text/plain content-type, got %q", ct)
	}
	body := w.Body.String()
	for _, want := range []string{
		"# HELP http_requests_total",
		"http_requests_total",
		"# HELP rate_limit_total",
		"rate_limit_total",
		"# HELP circuit_breaker_state",
		"# HELP http_request_duration_seconds",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
	// 验证限流/熔断埋点的具体值已出现在 exposition 文本中
	if !strings.Contains(body, `rate_limit_total{result="rejected",type="global"}`) {
		t.Errorf("rate_limit_total rejected counter not present in output:\n%s", body)
	}
	if !strings.Contains(body, `circuit_breaker_state{route="/api/v1/shop/redeem"} 2`) {
		t.Errorf("circuit_breaker_state gauge not present in output:\n%s", body)
	}
}

// mockL1 实现 L1Cache 接口的最小桩，供采集器测试（不依赖 Ristretto 实例）
type mockL1 struct{}

func (mockL1) Get(context.Context, string) (interface{}, bool, error)        { return nil, false, nil }
func (mockL1) Set(context.Context, string, interface{}, time.Duration) error { return nil }
func (mockL1) Delete(context.Context, ...string) error                       { return nil }
func (mockL1) Exists(context.Context, string) (bool, error)                  { return false, nil }
func (mockL1) GetMulti(context.Context, []string) (map[string]interface{}, error) {
	return nil, nil
}
func (mockL1) SetMulti(context.Context, map[string]interface{}, time.Duration) error { return nil }
func (mockL1) Size() int64                                                           { return 0 }
func (mockL1) HitRatio() float64                                                     { return 0.42 }
func (mockL1) Evictions() uint64                                                     { return 0 }
func (mockL1) Close()                                                                {}

func TestCacheCollectorCollect(t *testing.T) {
	mgr := &cache.Manager{L1: mockL1{}}
	col := &cacheCollector{mgr: mgr}

	ch := make(chan prometheus.Metric, 128)
	col.Collect(ch)
	close(ch)

	got := map[string]bool{}
	for m := range ch {
		got[metricName(m)] = true
	}

	for _, want := range []string{
		"cache_l1_enabled",
		"cache_l1_hits_total",
		"cache_l1_misses_total",
		"cache_l1_hit_ratio",
		"cache_l1_size_bytes",
		"cache_l1_evictions_total",
	} {
		if !got[want] {
			t.Errorf("cache collector missing metric %q (got: %v)", want, got)
		}
	}
}
