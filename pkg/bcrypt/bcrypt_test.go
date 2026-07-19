package bcrypt

import (
	"strings"
	"testing"
)

func TestHashAndCompare(t *testing.T) {
	password := "Sup3rSecret!"
	hash, err := Hash(password, DefaultCost)
	if err != nil {
		t.Fatalf("Hash failed: %v", err)
	}
	if hash == password {
		t.Fatal("Hash returned plaintext, expected a bcrypt hash")
	}
	if !strings.HasPrefix(hash, "$2a$") && !strings.HasPrefix(hash, "$2b$") {
		t.Fatalf("hash does not look like bcrypt: %q", hash)
	}

	if err := Compare(hash, password); err != nil {
		t.Fatalf("Compare with correct password should succeed, got %v", err)
	}
	if err := Compare(hash, "wrong-password"); err == nil {
		t.Fatal("Compare with wrong password should fail")
	}
}

func TestHashIsSalted(t *testing.T) {
	p := "same-password"
	h1, err := Hash(p, DefaultCost)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := Hash(p, DefaultCost)
	if err != nil {
		t.Fatal(err)
	}
	if h1 == h2 {
		t.Fatal("two hashes of the same password must differ (random salt)")
	}
}

func TestHashInvalidCost(t *testing.T) {
	// bcrypt 最大 cost 为 31，超过上限应返回 InvalidCost 错误，绝不能 panic。
	if _, err := Hash("pw", 32); err == nil {
		t.Fatal("Hash with cost 32 (> MaxCost 31) should return an error")
	}
}

func TestCompareInvalidHash(t *testing.T) {
	// 非法哈希串不应 panic，应返回错误。
	if err := Compare("not-a-valid-hash", "pw"); err == nil {
		t.Fatal("Compare with malformed hash should return an error")
	}
}
