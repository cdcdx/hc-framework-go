package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/cdcdx/hc-framework-go/internal/metrics"
)

func init() { gin.SetMode(gin.TestMode) }

// TestMetricsMiddleware_RecordsRequests 验证 Metrics 中间件会按 method/path/status
// 累积 http_requests_total，并观测请求延迟（http_request_duration_seconds）。
func TestMetricsMiddleware_RecordsRequests(t *testing.T) {
	r := gin.New()
	r.Use(Metrics())
	r.GET("/ping", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) })
	r.GET("/boom", func(c *gin.Context) { c.Status(http.StatusInternalServerError) })

	wOK := httptest.NewRecorder()
	r.ServeHTTP(wOK, httptest.NewRequest(http.MethodGet, "/ping", nil))
	if wOK.Code != http.StatusOK {
		t.Fatalf("/ping status = %d, want 200", wOK.Code)
	}

	wErr := httptest.NewRecorder()
	r.ServeHTTP(wErr, httptest.NewRequest(http.MethodGet, "/boom", nil))
	if wErr.Code != http.StatusInternalServerError {
		t.Fatalf("/boom status = %d, want 500", wErr.Code)
	}

	if got := testutil.ToFloat64(metrics.HTTPRequestsTotal.WithLabelValues("GET", "/ping", "200")); got < 1 {
		t.Errorf("http_requests_total{/ping,200} = %v, want >= 1", got)
	}
	if got := testutil.ToFloat64(metrics.HTTPRequestsTotal.WithLabelValues("GET", "/boom", "500")); got < 1 {
		t.Errorf("http_requests_total{/boom,500} = %v, want >= 1", got)
	}

	// 通过 Prometheus exposition 验证延迟直方图已被观测并暴露
	wM := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(wM, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := wM.Body.String()
	if !strings.Contains(body, `http_requests_total{method="GET",path="/ping",status="200"}`) {
		t.Errorf("exposition missing /ping 200 series:\n%s", body)
	}
	if !strings.Contains(body, "http_request_duration_seconds") {
		t.Errorf("exposition missing http_request_duration_seconds:\n%s", body)
	}
}
