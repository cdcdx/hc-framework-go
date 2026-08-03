// Package jwt JWT 签发与校验（go-zero 版，算法/claims 与 gin 版一致，
// 保证旧 token 可直接在网关校验）。
package jwt

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"time"

	jwtlib "github.com/golang-jwt/jwt/v5"
)

// Claims JWT 自定义声明
type Claims struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	jwtlib.RegisteredClaims
}

// Manager JWT 管理器
type Manager struct {
	algorithm  string
	signingKey string
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
	issuer     string
	accessTTL  time.Duration
	refreshTTL time.Duration
}

// NewManager 创建 JWT 管理器
func NewManager(algorithm, signingKey, privateKeyPath, publicKeyPath, issuer string, accessTTL, refreshTTL time.Duration) (*Manager, error) {
	m := &Manager{
		algorithm:  algorithm,
		signingKey: signingKey,
		issuer:     issuer,
		accessTTL:  accessTTL,
		refreshTTL: refreshTTL,
	}

	if algorithm == "RS256" {
		if err := m.loadRSAKeys(privateKeyPath, publicKeyPath); err != nil {
			return nil, err
		}
	}

	return m, nil
}

func (m *Manager) loadRSAKeys(privatePath, publicPath string) error {
	privData, err := os.ReadFile(privatePath)
	if err != nil {
		return fmt.Errorf("read private key: %w", err)
	}
	block, _ := pem.Decode(privData)
	if block == nil {
		return fmt.Errorf("decode private key failed")
	}
	priv, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		priv, err = x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return fmt.Errorf("parse private key: %w", err)
		}
	}
	rsaPriv, ok := priv.(*rsa.PrivateKey)
	if !ok {
		return fmt.Errorf("private key is not RSA: got %T", priv)
	}
	m.privateKey = rsaPriv

	if publicPath != "" {
		pubData, err := os.ReadFile(publicPath)
		if err != nil {
			return fmt.Errorf("read public key: %w", err)
		}
		block, _ = pem.Decode(pubData)
		if block == nil {
			return fmt.Errorf("decode public key failed")
		}
		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return fmt.Errorf("parse public key: %w", err)
		}
		rsaPub, ok := pub.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("public key is not RSA: got %T", pub)
		}
		m.publicKey = rsaPub
	} else {
		m.publicKey = &m.privateKey.PublicKey
	}

	return nil
}

// GenerateAccessToken 生成 Access Token
func (m *Manager) GenerateAccessToken(userID, email string) (string, error) {
	now := time.Now()
	claims := &Claims{
		UserID: userID,
		Email:  email,
		RegisteredClaims: jwtlib.RegisteredClaims{
			Issuer:    m.issuer,
			Subject:   userID,
			IssuedAt:  jwtlib.NewNumericDate(now),
			ExpiresAt: jwtlib.NewNumericDate(now.Add(m.accessTTL)),
			ID:        fmt.Sprintf("%s-%d", userID, now.UnixNano()),
		},
	}
	return m.sign(claims)
}

// GenerateRefreshToken 生成 Refresh Token
func (m *Manager) GenerateRefreshToken(userID, email string) (string, error) {
	now := time.Now()
	claims := &Claims{
		UserID: userID,
		Email:  email,
		RegisteredClaims: jwtlib.RegisteredClaims{
			Issuer:    m.issuer,
			Subject:   userID,
			IssuedAt:  jwtlib.NewNumericDate(now),
			ExpiresAt: jwtlib.NewNumericDate(now.Add(m.refreshTTL)),
			ID:        fmt.Sprintf("refresh-%s-%d", userID, now.UnixNano()),
		},
	}
	return m.sign(claims)
}

// GenerateTokenPair 同时生成 Access + Refresh Token
func (m *Manager) GenerateTokenPair(userID, email string) (accessToken, refreshToken string, err error) {
	accessToken, err = m.GenerateAccessToken(userID, email)
	if err != nil {
		return "", "", err
	}
	refreshToken, err = m.GenerateRefreshToken(userID, email)
	if err != nil {
		return "", "", err
	}
	return
}

// ValidateToken 验证 Token，返回 Claims
func (m *Manager) ValidateToken(tokenString string) (*Claims, error) {
	token, err := jwtlib.ParseWithClaims(tokenString, &Claims{}, m.keyFunc)
	if err != nil {
		return nil, fmt.Errorf("parse token: %w", err)
	}
	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}
	return claims, nil
}

func (m *Manager) sign(claims *Claims) (string, error) {
	token := jwtlib.NewWithClaims(jwtlib.SigningMethodHS256, claims)
	if m.algorithm == "RS256" {
		token = jwtlib.NewWithClaims(jwtlib.SigningMethodRS256, claims)
	}

	var key interface{}
	if m.algorithm == "RS256" {
		key = m.privateKey
	} else {
		key = []byte(m.signingKey)
	}

	return token.SignedString(key)
}

func (m *Manager) keyFunc(token *jwtlib.Token) (interface{}, error) {
	if m.algorithm == "RS256" {
		if _, ok := token.Method.(*jwtlib.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return m.publicKey, nil
	}
	if _, ok := token.Method.(*jwtlib.SigningMethodHMAC); !ok {
		return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
	}
	return []byte(m.signingKey), nil
}

// context key：网关 JwtAuth 中间件把解析出的 user_id 注入 request context。
type ctxKey struct{}

var userIDKey ctxKey

// WithUserID 把 user_id 写入 context
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDKey, userID)
}

// UserIDFromContext 从 context 提取 user_id（网关 handler 内调用）
func UserIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(userIDKey).(string)
	return id, ok
}
