package middleware

import (
	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/gin-gonic/gin"
)

// CORS 跨域中间件
func CORS(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")
		if origin == "" {
			c.Next()
			return
		}

		allowed := false
		for _, o := range cfg.Server.CORS.AllowedOrigins {
			if o == "*" || o == origin {
				allowed = true
				break
			}
		}

		if !allowed {
			c.AbortWithStatus(403)
			return
		}

		c.Header("Access-Control-Allow-Origin", origin)
		c.Header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Authorization,Content-Type,X-Trace-Id")
		c.Header("Access-Control-Expose-Headers", "X-Trace-Id,X-RateLimit-Limit,X-RateLimit-Remaining,X-RateLimit-Reset")
		c.Header("Access-Control-Max-Age", "3600")
		c.Header("Content-Security-Policy", "default-src 'self'")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}
