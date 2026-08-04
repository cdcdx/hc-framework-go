package jwt

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func hsManager(t *testing.T) *Manager {
	t.Helper()
	m, err := NewManager("HS256", "test-secret-key-at-least-32-bytes-long", "", "", "hc-test",
		time.Minute, time.Hour)
	if err != nil {
		t.Fatalf("NewManager HS256: %v", err)
	}
	return m
}

func rsManager(t *testing.T) (*Manager, func()) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen rsa: %v", err)
	}
	dir := t.TempDir()
	privPath := filepath.Join(dir, "priv.pem")
	pubPath := filepath.Join(dir, "pub.pem")
	privPem := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	pubBytes, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		t.Fatalf("marshal pub: %v", err)
	}
	pubPem := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubBytes})
	if err := os.WriteFile(privPath, privPem, 0o600); err != nil {
		t.Fatalf("write priv: %v", err)
	}
	if err := os.WriteFile(pubPath, pubPem, 0o644); err != nil {
		t.Fatalf("write pub: %v", err)
	}
	m, err := NewManager("RS256", "", privPath, pubPath, "hc-test", time.Minute, time.Hour)
	if err != nil {
		t.Fatalf("NewManager RS256: %v", err)
	}
	return m, func() {}
}

func TestManager_TokenRoundTrip_HS256(t *testing.T) {
	m := hsManager(t)
	token, err := m.GenerateAccessToken("u1", "u1@example.com")
	if err != nil {
		t.Fatalf("GenerateAccessToken: %v", err)
	}
	claims, err := m.ValidateToken(token)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if claims.UserID != "u1" {
		t.Errorf("UserID = %q, want u1", claims.UserID)
	}
	if claims.Email != "u1@example.com" {
		t.Errorf("Email = %q, want u1@example.com", claims.Email)
	}
	if claims.Issuer != "hc-test" {
		t.Errorf("Issuer = %q, want hc-test", claims.Issuer)
	}
}

func TestManager_TokenRoundTrip_RS256(t *testing.T) {
	m, cleanup := rsManager(t)
	defer cleanup()
	token, err := m.GenerateAccessToken("u2", "u2@example.com")
	if err != nil {
		t.Fatalf("GenerateAccessToken: %v", err)
	}
	claims, err := m.ValidateToken(token)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if claims.UserID != "u2" {
		t.Errorf("UserID = %q, want u2", claims.UserID)
	}
}

func TestManager_ExpiredToken(t *testing.T) {
	m, err := NewManager("HS256", "test-secret-key-at-least-32-bytes-long", "", "", "hc-test",
		-time.Minute, time.Hour) // negative TTL => already expired
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	token, err := m.GenerateAccessToken("u1", "u1@x.com")
	if err != nil {
		t.Fatalf("GenerateAccessToken: %v", err)
	}
	if _, err := m.ValidateToken(token); err == nil {
		t.Fatal("expected expired token error, got nil")
	}
}

func TestManager_AlgorithmConfusion_HS256TokenVerifiedByRS256Key(t *testing.T) {
	// RS256 manager must reject an HS256-signed token (alg confusion attack).
	rs, cleanup := rsManager(t)
	defer cleanup()
	hs := hsManager(t)

	hsToken, err := hs.GenerateAccessToken("attacker", "a@x.com")
	if err != nil {
		t.Fatalf("hs token: %v", err)
	}
	if _, err := rs.ValidateToken(hsToken); err == nil {
		t.Fatal("RS256 manager accepted an HS256 token (alg confusion) — VULNERABLE")
	}
}

func TestManager_RefreshTokenDistinct(t *testing.T) {
	m := hsManager(t)
	at, rt, err := m.GenerateTokenPair("u1", "u1@x.com")
	if err != nil {
		t.Fatalf("GenerateTokenPair: %v", err)
	}
	if at == rt {
		t.Fatal("access and refresh tokens must differ")
	}
	if _, err := m.ValidateToken(rt); err != nil {
		t.Fatalf("refresh token should validate: %v", err)
	}
}

func TestManager_InvalidToken(t *testing.T) {
	m := hsManager(t)
	if _, err := m.ValidateToken("not-a-jwt"); err == nil {
		t.Fatal("expected error for malformed token")
	}
}

func TestManager_NewManagerMissingRSAKeyPath(t *testing.T) {
	if _, err := NewManager("RS256", "", "/nonexistent/priv.pem", "", "hc-test", time.Minute, time.Hour); err == nil {
		t.Fatal("expected error when RS256 key path missing")
	}
}

func TestContextUserID(t *testing.T) {
	ctx := WithUserID(context.Background(), "u42")
	id, ok := UserIDFromContext(ctx)
	if !ok || id != "u42" {
		t.Errorf("UserIDFromContext = (%q,%v), want (u42,true)", id, ok)
	}
	if _, ok := UserIDFromContext(context.Background()); ok {
		t.Error("expected ok=false for empty context")
	}
}
