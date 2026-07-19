package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/gin-gonic/gin"
)

func setupCORSGin(cfg *config.Config) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(CORS(cfg))
	r.GET("/ok", func(c *gin.Context) { c.Status(200) })
	return r
}

func TestCORS_NoOrigin_PassThrough(t *testing.T) {
	r := setupCORSGin(&config.Config{
		Server: config.ServerConfig{CORS: config.CORSConfig{AllowedOrigins: []string{"*"}}},
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/ok", nil)
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200 without Origin, got %d", w.Code)
	}
}

func TestCORS_AllowedOrigin(t *testing.T) {
	r := setupCORSGin(&config.Config{
		Server: config.ServerConfig{CORS: config.CORSConfig{AllowedOrigins: []string{"https://example.com"}}},
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/ok", nil)
	req.Header.Set("Origin", "https://example.com")
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200 for allowed origin, got %d", w.Code)
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "https://example.com" {
		t.Error("missing Access-Control-Allow-Origin header")
	}
}

func TestCORS_WildcardAllowsAny(t *testing.T) {
	r := setupCORSGin(&config.Config{
		Server: config.ServerConfig{CORS: config.CORSConfig{AllowedOrigins: []string{"*"}}},
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/ok", nil)
	req.Header.Set("Origin", "https://any.example.com")
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200 with wildcard CORS, got %d", w.Code)
	}
}

func TestCORS_DisallowedOrigin_Returns403(t *testing.T) {
	r := setupCORSGin(&config.Config{
		Server: config.ServerConfig{CORS: config.CORSConfig{AllowedOrigins: []string{"https://example.com"}}},
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/ok", nil)
	req.Header.Set("Origin", "https://evil.com")
	r.ServeHTTP(w, req)
	if w.Code != 403 {
		t.Fatalf("expected 403 for disallowed origin, got %d", w.Code)
	}
}

func TestCORS_OPTIONS_Preflight(t *testing.T) {
	r := setupCORSGin(&config.Config{
		Server: config.ServerConfig{CORS: config.CORSConfig{AllowedOrigins: []string{"*"}}},
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("OPTIONS", "/ok", nil)
	req.Header.Set("Origin", "https://example.com")
	r.ServeHTTP(w, req)
	if w.Code != 204 {
		t.Fatalf("expected 204 for OPTIONS preflight, got %d", w.Code)
	}
}

func TestCORS_SetsSecurityHeaders(t *testing.T) {
	r := setupCORSGin(&config.Config{
		Server: config.ServerConfig{CORS: config.CORSConfig{AllowedOrigins: []string{"*"}}},
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/ok", nil)
	req.Header.Set("Origin", "https://example.com")
	r.ServeHTTP(w, req)
	if w.Header().Get("Content-Security-Policy") == "" {
		t.Error("missing CSP header")
	}
	if w.Header().Get("Access-Control-Max-Age") == "" {
		t.Error("missing Max-Age header")
	}
}

func TestCORS_ExposeRateLimitHeaders(t *testing.T) {
	r := setupCORSGin(&config.Config{
		Server: config.ServerConfig{CORS: config.CORSConfig{AllowedOrigins: []string{"*"}}},
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/ok", nil)
	req.Header.Set("Origin", "https://example.com")
	r.ServeHTTP(w, req)
	expose := w.Header().Get("Access-Control-Expose-Headers")
	if expose == "" {
		t.Fatal("missing Expose-Headers")
	}
	// 应包含限流相关头
	required := []string{"X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"}
	for _, h := range required {
		if !containsHeader(expose, h) {
			t.Errorf("Expose-Headers missing %s: got %s", h, expose)
		}
	}
}

func containsHeader(expose, target string) bool {
	return len(expose) > 0 && (expose == target || len(expose) > len(target) && containsWord(expose, target))
}

func containsWord(s, word string) bool {
	for i := 0; i <= len(s)-len(word); i++ {
		if s[i:i+len(word)] == word {
			return true
		}
	}
	return false
}
