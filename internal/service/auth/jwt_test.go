package auth

import (
	"sync"
	"testing"
	"time"
)

func resetJWTGlobal() {
	jwtMgr = nil
	jwtMgrOnce = sync.Once{}
}

func TestJWTGenerate_NotInitialized(t *testing.T) {
	resetJWTGlobal()
	_, _, err := jwtGenerate("u1", "u1@test.com")
	if err == nil {
		t.Fatal("expected error when JWT not initialized")
	}
}

func TestJWTValidate_NotInitialized(t *testing.T) {
	resetJWTGlobal()
	_, err := jwtValidate("some-token")
	if err == nil {
		t.Fatal("expected error when JWT not initialized")
	}
}

func TestInitJWT_HS256_GenerateAndValidate(t *testing.T) {
	resetJWTGlobal()
	err := InitJWT("HS256", "test-signing-key-32bytes-long!!", "", "", "test-issuer", 1*time.Hour, 24*time.Hour)
	if err != nil {
		t.Fatalf("InitJWT: %v", err)
	}
	// GenerateTokenPair
	access, refresh, err := jwtGenerate("u1", "u1@test.com")
	if err != nil {
		t.Fatalf("jwtGenerate: %v", err)
	}
	if access == "" || refresh == "" {
		t.Fatal("tokens must not be empty")
	}
	// Validate
	claims, err := jwtValidate(access)
	if err != nil {
		t.Fatalf("jwtValidate: %v", err)
	}
	if claims.UserID != "u1" || claims.Email != "u1@test.com" || claims.Issuer != "test-issuer" {
		t.Error("claims mismatch")
	}
	// Different users get different tokens
	a2, _, _ := jwtGenerate("u2", "u2@test.com")
	if access == a2 {
		t.Fatal("different users should get different tokens")
	}
	// InitJWT 幂等（sync.Once，第二次调用返回缓存结果）
	err2 := InitJWT("HS256", "another-key-32bytes-long-ok!!", "", "", "x", 1*time.Hour, 1*time.Hour)
	if err2 != nil {
		t.Fatalf("second InitJWT (sync.Once) should return nil: %v", err2)
	}
}
