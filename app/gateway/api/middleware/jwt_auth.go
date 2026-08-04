package middleware

import (
	"errors"
	"net/http"
	"strings"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/jwt"
	"github.com/cdcdx/hc-framework-go/common/response"
	jwtlib "github.com/golang-jwt/jwt/v5"
)

// JwtAuthMiddleware 网关鉴权：校验 Authorization: Bearer <token>，
// 通过后把 user_id 注入 request context（logic 内用 jwt.UserIDFromContext 取）。
type JwtAuthMiddleware struct {
	svcCtx *svc.ServiceContext
}

// NewJwtAuthMiddleware 创建鉴权中间件
func NewJwtAuthMiddleware(svcCtx *svc.ServiceContext) *JwtAuthMiddleware {
	return &JwtAuthMiddleware{svcCtx: svcCtx}
}

// Handle 中间件处理
func (m *JwtAuthMiddleware) Handle(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := extractBearer(r.Header.Get("Authorization"))
		if token == "" {
			response.Unauthorized(w, r, errorx.CodeTokenInvalid)
			return
		}

		claims, err := m.svcCtx.JwtMgr.ValidateToken(token)
		if err != nil {
			code := errorx.CodeTokenInvalid
			if errors.Is(err, jwtlib.ErrTokenExpired) {
				code = errorx.CodeTokenExpired
			}
			response.Unauthorized(w, r, code)
			return
		}

		ctx := jwt.WithUserID(r.Context(), claims.UserID)
		next(w, r.WithContext(ctx))
	}
}

// extractBearer 从 Authorization 头提取 Bearer token
func extractBearer(auth string) string {
	const prefix = "Bearer "
	if strings.HasPrefix(auth, prefix) {
		return strings.TrimSpace(auth[len(prefix):])
	}
	return ""
}
