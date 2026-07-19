package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/middleware"
	"github.com/cdcdx/hc-framework-go/pkg/response"
	"github.com/gin-gonic/gin"
	jwtlib "github.com/golang-jwt/jwt/v5"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// stubAuthServer 构造使用「桩验证器」的鉴权服务器，隔离中间件自身开销（不含真实 HMAC 验签）。
func stubAuthServer(tb testing.TB, validate func(string) (*middleware.JWTClaims, error)) *gin.Engine {
	tb.Helper()
	middleware.SetJWTValidator(validate)
	r := gin.New()
	r.Use(middleware.Auth(&config.Config{}))
	r.GET("/me", func(c *gin.Context) {
		response.Success(c, gin.H{"user_id": "u1"})
	})
	return r
}

// benchAuth 以并发方式压测一次完整中间件链路，并报告每次操作的内存分配。
func benchAuth(b *testing.B, srv *gin.Engine, header string) {
	req := httptest.NewRequest(http.MethodGet, "/me", nil)
	if header != "" {
		req.Header.Set("Authorization", header)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, req)
		}
	})
}

// BenchmarkAuthMiddleware_Valid 合法 Token 路径：验证 strings.Cut 提取 + c.Set 注入的开销。
func BenchmarkAuthMiddleware_Valid(b *testing.B) {
	srv := stubAuthServer(b, func(string) (*middleware.JWTClaims, error) {
		return &middleware.JWTClaims{UserID: "u1", Email: "u1@x.com"}, nil
	})
	benchAuth(b, srv, "Bearer valid-token")
}

// BenchmarkAuthMiddleware_Expired 过期 Token 路径：验证 errors.As 解包判断，
// 不再走 err.Error() 整串分配（与旧实现的 strings.Contains(err.Error(),...) 对比）。
func BenchmarkAuthMiddleware_Expired(b *testing.B) {
	srv := stubAuthServer(b, func(string) (*middleware.JWTClaims, error) {
		return nil, jwtlib.ErrTokenExpired
	})
	benchAuth(b, srv, "Bearer expired-token")
}

// BenchmarkAuthMiddleware_Invalid 非法 Token 路径：落到字符串回退分支（非标准错误），作为对照。
func BenchmarkAuthMiddleware_Invalid(b *testing.B) {
	srv := stubAuthServer(b, func(string) (*middleware.JWTClaims, error) {
		return nil, jwtlib.ErrSignatureInvalid
	})
	benchAuth(b, srv, "Bearer bad-token")
}
