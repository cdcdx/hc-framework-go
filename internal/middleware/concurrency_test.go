package middleware

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/gin-gonic/gin"
)

// TestConcurrencyLimit_RejectsWhenFull 验证：达到并发上限后，超额请求被快速失败返回 503
// （负载卸载），而系统端点（/health）即使在高负载下仍被豁免、正常返回 200。
func TestConcurrencyLimit_RejectsWhenFull(t *testing.T) {
	cfg := &config.Config{}
	cfg.Server.ConcurrencyLimit = 2
	mgr := config.NewManager(cfg)
	mw := ConcurrencyLimit(mgr)

	release := make(chan struct{})
	reached := make(chan struct{}, 3)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(mw)
	r.GET("/x", func(c *gin.Context) {
		reached <- struct{}{}
		<-release
		c.Status(http.StatusOK)
	})
	r.GET("/health", func(c *gin.Context) { c.Status(http.StatusOK) })

	srv := httptest.NewServer(r)
	defer srv.Close()

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = http.Get(srv.URL + "/x")
		}()
	}
	// 等待两个请求进入在途（占用全部 2 个配额）
	for i := 0; i < 2; i++ {
		<-reached
	}

	// 第三个请求应被并发上限拦截，立即返回 503
	resp, err := http.Get(srv.URL + "/x")
	if err != nil {
		t.Fatalf("third request error: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 (load shedding), got %d", resp.StatusCode)
	}

	// 系统端点豁免：即便并发已满，/health 仍应 200
	hresp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("health request error: %v", err)
	}
	if hresp.StatusCode != http.StatusOK {
		t.Fatalf("expected /health 200 under load, got %d", hresp.StatusCode)
	}

	close(release)
	wg.Wait()
}

// TestConcurrencyLimit_Disabled 验证：concurrency_limit<=0 时中间件直通，不拒绝任何请求。
func TestConcurrencyLimit_Disabled(t *testing.T) {
	cfg := &config.Config{} // ConcurrencyLimit 零值 = 未启用
	mgr := config.NewManager(cfg)
	mw := ConcurrencyLimit(mgr)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(mw)
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	srv := httptest.NewServer(r)
	defer srv.Close()

	// 远超任何隐含上限的并发也应全部 200（未启用时无限）
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get(srv.URL + "/x")
			if err != nil {
				t.Errorf("request error: %v", err)
				return
			}
			if resp.StatusCode != http.StatusOK {
				t.Errorf("expected 200, got %d", resp.StatusCode)
			}
		}()
	}
	wg.Wait()
}
