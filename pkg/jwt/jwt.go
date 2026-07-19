// Package jwt JWT 签发与校验。
package jwt

import (
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
	// 密钥由外部（不可信）配置文件指定，类型可能非 RSA（如误配 EC 密钥）；
	// 裸断言会在启动期 panic，故加 ok 守卫转友好 error（13 §3.46）。
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
		// 同上：公钥类型可能非 RSA，裸断言会 panic，转友好 error（13 §3.46）。
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
