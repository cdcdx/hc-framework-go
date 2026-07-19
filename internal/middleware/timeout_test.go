package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/gin-gonic/gin"
)

// TestTimeout_SlowHandlerReturns504 模拟「连接池饱和 → DB 查询 ctx 取消」：
// 慢 handler 在 deadline 到达后随 ctx 取消退出，中间件应回 504 而非无限挂起（13 §3.48）。
func TestTimeout_SlowHandlerReturns504(t *testing.T) {
	cfg := &config.Config{}
	cfg.Server.RequestTimeout = 50 * time.Millisecond

	r := gin.New()
	r.Use(Timeout(cfg))
	r.GET("/slow", func(c *gin.Context) {
		select {
		case <-time.After(2 * time.Second):
			c.JSON(http.StatusOK, gin.H{"ok": true})
		case <-c.Request.Context().Done():
			return // 模拟 database/sql 在 ctx 取消后停止等待空闲连接
		}
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/slow", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504 (gateway timeout)", w.Code)
	}
}

// TestTimeout_FastHandlerOK 快速 handler 不应被超时影响。
func TestTimeout_FastHandlerOK(t *testing.T) {
	cfg := &config.Config{}
	cfg.Server.RequestTimeout = 50 * time.Millisecond

	r := gin.New()
	r.Use(Timeout(cfg))
	r.GET("/fast", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/fast", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

// TestTimeout_DefaultWhenZero 未配置 request_timeout 时使用默认 10s 兜底，快速请求不受影响。
func TestTimeout_DefaultWhenZero(t *testing.T) {
	cfg := &config.Config{} // RequestTimeout == 0

	r := gin.New()
	r.Use(Timeout(cfg))
	r.GET("/fast", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/fast", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (default timeout must not fire for fast handler)", w.Code)
	}
}
