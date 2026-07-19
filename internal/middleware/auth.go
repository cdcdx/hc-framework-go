// Package middleware HTTP 中间件集合（鉴权、限流、熔断、CORS、链路追踪等）。
package middleware

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	jwtlib "github.com/golang-jwt/jwt/v5"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/pkg/response"
)

// JWTManager JWT 管理器接口（避免循环 import）
type JWTManager interface {
	ValidateToken(tokenString string) (*JWTClaims, error)
}

// JWTClaims JWT 声明
type JWTClaims struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	jwtlib.RegisteredClaims
}

// TokenBlacklistChecker Token 黑名单检查接口
type TokenBlacklistChecker interface {
	IsBlacklisted(ctx context.Context, jti string) (bool, error)
	// IsBlacklistedMulti 批量检查多个键是否黑名单命中，返回各键结果（单次 RTT）。
	IsBlacklistedMulti(ctx context.Context, jtis ...string) ([]bool, error)
	AddBlacklist(ctx context.Context, jti string, ttl time.Duration) error
}

var jwtValidator func(tokenString string) (*JWTClaims, error)

// SetJWTValidator 设置 JWT 验证函数（由 main 注入）
func SetJWTValidator(fn func(tokenString string) (*JWTClaims, error)) {
	jwtValidator = fn
}

var blacklistChecker TokenBlacklistChecker

// SetBlacklistChecker 设置黑名单检查器（由 main 注入 cache.Manager）
func SetBlacklistChecker(checker TokenBlacklistChecker) {
	blacklistChecker = checker
}

// Auth JWT 鉴权中间件
func Auth(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			response.Unauthorized(c, 10102)
			c.Abort()
			return
		}

		// 提取 Token（支持 "Bearer xxx" 和直接 "xxx" 两种格式）
		tokenString := authHeader
		if scheme, rest, ok := strings.Cut(authHeader, " "); ok && strings.EqualFold(scheme, "Bearer") {
			tokenString = rest
		}

		// 验证 JWT Token
		if jwtValidator == nil {
			// JWT validator 未初始化，降级为简单解析
			c.Set("user_id", "anonymous")
			c.Next()
			return
		}

		claims, err := jwtValidator(tokenString)
		if err != nil {
			// 判断错误类型：优先用 errors.Is 匹配 golang-jwt v5 的 ErrTokenExpired 哨兵
			// （golang-jwt v5 已移除 v4 的 *ValidationError 类型，改为哨兵错误；本处对 %w
			// 包裹透明），避免 err.Error() 分配整串字符串 + 子串扫描；匹配失败时回退到
			// 原有字符串匹配以兼容非标准错误。
			var expired bool
			if errors.Is(err, jwtlib.ErrTokenExpired) {
				expired = true
			} else {
				expired = strings.Contains(err.Error(), "expired")
			}
			if expired {
				response.Unauthorized(c, 10101) // Token 过期
			} else {
				response.Unauthorized(c, 10102) // Token 无效
			}
			c.Abort()
			return
		}

		// 合并黑名单检查：单次多 key EXISTS（Pipeline，单次 RTT）同时判定 jti 与
		// pwd_change 两个键，避免两次独立 Redis 往返。jti 命中即失效；pwd_change 命中
		// 仍需满足 claims.IssuedAt != nil（与原两次独立检查语义一致）。
		if blacklistChecker != nil {
			var keys []string
			jtiIdx, pwdIdx := -1, -1
			if claims.ID != "" {
				keys = append(keys, claims.ID)
				jtiIdx = len(keys) - 1
			}
			if cfg.Security.TokenBlacklistEnabled {
				keys = append(keys, "pwd_change:"+claims.UserID)
				pwdIdx = len(keys) - 1
			}
			if len(keys) > 0 {
				if existed, err := blacklistChecker.IsBlacklistedMulti(c.Request.Context(), keys...); err == nil {
					if jtiIdx >= 0 && existed[jtiIdx] {
						response.Unauthorized(c, 10104) // Token 已失效
						c.Abort()
						return
					}
					if pwdIdx >= 0 && existed[pwdIdx] && claims.IssuedAt != nil {
						response.Unauthorized(c, 10104) // Token 已失效（密码修改后）
						c.Abort()
						return
					}
				}
			}
		}

		// 注入 user_id 和 email 到 context
		c.Set("user_id", claims.UserID)
		c.Set("email", claims.Email)
		c.Set("token_jti", claims.ID)
		if claims.ExpiresAt != nil {
			c.Set("token_expires_at", claims.ExpiresAt.Time)
		}

		c.Next()
	}
}
