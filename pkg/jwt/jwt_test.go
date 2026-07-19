package jwt

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newHMACManager(t *testing.T) *Manager {
	t.Helper()
	m, err := NewManager("HS256", "test-signing-secret", "", "", "hc-framework", 15*time.Minute, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("NewManager HS256: %v", err)
	}
	return m
}

func TestHMACRoundtrip(t *testing.T) {
	m := newHMACManager(t)
	access, refresh, err := m.GenerateTokenPair("user-123", "u@example.com")
	if err != nil {
		t.Fatalf("GenerateTokenPair: %v", err)
	}
	if access == "" || refresh == "" {
		t.Fatal("tokens must not be empty")
	}

	claims, err := m.ValidateToken(access)
	if err != nil {
		t.Fatalf("ValidateToken(access): %v", err)
	}
	if claims.UserID != "user-123" {
		t.Fatalf("UserID = %q, want user-123", claims.UserID)
	}
	if claims.Email != "u@example.com" {
		t.Fatalf("Email = %q, want u@example.com", claims.Email)
	}
	if claims.Issuer != "hc-framework" {
		t.Fatalf("Issuer = %q, want hc-framework", claims.Issuer)
	}

	if _, err := m.ValidateToken(refresh); err != nil {
		t.Fatalf("ValidateToken(refresh): %v", err)
	}
}

func TestHMACWrongKeyRejected(t *testing.T) {
	m1, _ := NewManager("HS256", "secret-a", "", "", "iss", time.Minute, time.Hour)
	m2, _ := NewManager("HS256", "secret-b", "", "", "iss", time.Minute, time.Hour)

	tok, err := m1.GenerateAccessToken("u", "u@x.com")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m2.ValidateToken(tok); err == nil {
		t.Fatal("token signed with a different key must be rejected")
	}
}

func TestTokenExpiredRejected(t *testing.T) {
	// accessTTL 设为负值 => 签发时 ExpiresAt 已过期，校验应失败。
	m, err := NewManager("HS256", "secret", "", "", "iss", -time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := m.GenerateAccessToken("u", "u@x.com")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.ValidateToken(tok); err == nil {
		t.Fatal("expired token must be rejected")
	}
}

func TestTamperedTokenRejected(t *testing.T) {
	m := newHMACManager(t)
	tok, err := m.GenerateAccessToken("u", "u@x.com")
	if err != nil {
		t.Fatal(err)
	}
	// 篡改最后一个字符（保持合法 base64 外观）。
	tampered := tok[:len(tok)-1] + "X"
	if _, err := m.ValidateToken(tampered); err == nil {
		t.Fatal("tampered token must be rejected")
	}
}

func newRS256Manager(t *testing.T) *Manager {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen rsa: %v", err)
	}
	privPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	pubDER, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		t.Fatalf("marshal pub: %v", err)
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})

	dir := t.TempDir()
	privPath := filepath.Join(dir, "priv.pem")
	pubPath := filepath.Join(dir, "pub.pem")
	if err := os.WriteFile(privPath, privPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pubPath, pubPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	m, err := NewManager("RS256", "", privPath, pubPath, "hc-framework", time.Minute, time.Hour)
	if err != nil {
		t.Fatalf("NewManager RS256: %v", err)
	}
	return m
}

func TestRS256Roundtrip(t *testing.T) {
	m := newRS256Manager(t)
	tok, err := m.GenerateAccessToken("u", "u@x.com")
	if err != nil {
		t.Fatalf("GenerateAccessToken RS256: %v", err)
	}
	claims, err := m.ValidateToken(tok)
	if err != nil {
		t.Fatalf("ValidateToken RS256: %v", err)
	}
	if claims.UserID != "u" {
		t.Fatalf("UserID = %q, want u", claims.UserID)
	}
}

func TestRS256RejectsHMACToken(t *testing.T) {
	rsaMgr := newRS256Manager(t)
	hmacMgr := newHMACManager(t)

	// 用 HMAC 签发的 token 交给 RS256 管理器校验，应因算法不匹配被拒。
	hmacTok, err := hmacMgr.GenerateAccessToken("u", "u@x.com")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rsaMgr.ValidateToken(hmacTok); err == nil {
		t.Fatal("RS256 manager must reject an HMAC-signed token (algorithm confusion)")
	}
}
