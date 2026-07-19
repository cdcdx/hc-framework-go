package auth

import (
	"fmt"
	"sync"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/middleware"
	pkgjwt "github.com/cdcdx/hc-framework-go/pkg/jwt"
)

var (
	jwtMgr     *pkgjwt.Manager
	jwtMgrOnce sync.Once
)

// InitJWT 初始化 JWT 管理器
func InitJWT(algorithm, signingKey, privateKeyPath, publicKeyPath, issuer string, accessTTL, refreshTTL time.Duration) error {
	var err error
	jwtMgrOnce.Do(func() {
		jwtMgr, err = pkgjwt.NewManager(algorithm, signingKey, privateKeyPath, publicKeyPath, issuer, accessTTL, refreshTTL)
		if err == nil {
			// 注册 JWT 验证函数到中间件
			middleware.SetJWTValidator(func(tokenString string) (*middleware.JWTClaims, error) {
				claims, err := jwtMgr.ValidateToken(tokenString)
				if err != nil {
					return nil, err
				}
				return &middleware.JWTClaims{
					UserID: claims.UserID,
					Email:  claims.Email,
				}, nil
			})
		}
	})
	return err
}

// jwtGenerate 生成 Token 对
func jwtGenerate(userID, email string) (string, string, error) {
	if jwtMgr == nil {
		return "", "", fmt.Errorf("jwt manager not initialized")
	}
	return jwtMgr.GenerateTokenPair(userID, email)
}

// jwtValidate 验证 Token
func jwtValidate(tokenString string) (*pkgjwt.Claims, error) {
	if jwtMgr == nil {
		return nil, fmt.Errorf("jwt manager not initialized")
	}
	return jwtMgr.ValidateToken(tokenString)
}
