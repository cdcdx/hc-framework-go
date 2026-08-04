package bcrypt

import "testing"

func TestHashAndCompare(t *testing.T) {
	tests := []struct {
		name string
		pwd  string
		cost int
	}{
		{"default cost", "secret123", 0},
		{"explicit cost", "hunter2!", 10},
		{"unicode pwd", "密码🔒", 12},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash, err := Hash(tt.pwd, tt.cost)
			if err != nil {
				t.Fatalf("Hash: %v", err)
			}
			if hash == tt.pwd {
				t.Fatal("hash must not equal plaintext")
			}
			if err := Compare(hash, tt.pwd); err != nil {
				t.Errorf("Compare(valid) = %v, want nil", err)
			}
			if err := Compare(hash, "wrong-password"); err == nil {
				t.Error("Compare(wrong) should fail")
			}
		})
	}
}

func TestHash_Unique(t *testing.T) {
	h1, _ := Hash("same", 0)
	h2, _ := Hash("same", 0)
	if h1 == h2 {
		t.Fatal("bcrypt hashes of same password must differ (salt)")
	}
}
