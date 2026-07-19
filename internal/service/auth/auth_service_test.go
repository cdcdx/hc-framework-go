package auth

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// loadPasswordTestManager 构造带 auth.password 配置的最小配置管理器。
// passwordYAML 为 auth.password 段的内容（按 2 空格缩进）。
func loadPasswordTestManager(t *testing.T, passwordYAML string) *config.Manager {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	body := fmt.Sprintf(`
server:
  port: 8080
  mode: test
auth:
  password:
%s
`, passwordYAML)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	m, err := config.LoadManager(p)
	if err != nil {
		t.Fatalf("load manager: %v", err)
	}
	return m
}

// defaultPasswordYAML 对应 config.yaml 默认强度策略（需求 §8：≥8 位，四类至少满足 3 种）。
const defaultPasswordYAML = `
    bcrypt_cost: 10
    min_length: 8
    require_upper: true
    require_lower: true
    require_digit: true
    require_special: true
    require_categories: 3
`

// TestValidatePassword_StrengthPolicy 表驱动验证默认强度策略（需求 §8）。
func TestValidatePassword_StrengthPolicy(t *testing.T) {
	svc := &AuthService{cfg: loadPasswordTestManager(t, defaultPasswordYAML)}

	cases := []struct {
		name     string
		password string
		wantErr  bool
	}{
		// 太短（< min_length=8）
		{"too_short", "Ab1!", true},
		// 仅小写，类别数=1 < 3
		{"only_lower", "abcdefgh", true},
		// 小写+数字，类别数=2 < 3
		{"lower_digit", "abcdefg1", true},
		// 大写+小写+数字，类别数=3，刚好达标
		{"upper_lower_digit", "Abcdefg1", false},
		// 四类齐全，达标
		{"all_categories", "Abcdef12!", false},
		// 正好 8 位且达标
		{"exactly_min_length", "Abcd123!", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := svc.validatePassword(tc.password)
			if tc.wantErr && err == nil {
				t.Fatalf("validatePassword(%q) = nil, want error", tc.password)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("validatePassword(%q) = %v, want nil", tc.password, err)
			}
		})
	}
}

// TestValidatePassword_ClientHashedHex 客户端预哈希（64 位十六进制 SHA256）走 hex 校验分支，
// 不执行强度策略（设计约定：注册/登录须使用同一形式）。
func TestValidatePassword_ClientHashedHex(t *testing.T) {
	svc := &AuthService{cfg: loadPasswordTestManager(t, defaultPasswordYAML)}

	// 合法 64 位 hex：即使内容是弱口令的哈希，也通过（仅校验 hex 格式）。
	validHex := strings.Repeat("a", 64)
	if err := svc.validatePassword(validHex); err != nil {
		t.Fatalf("valid 64-hex should pass, got %v", err)
	}

	// 非 hex 的 64 位字符串：应报 hex 格式错误。
	invalidHex := strings.Repeat("g", 64)
	if err := svc.validatePassword(invalidHex); err == nil {
		t.Fatal("64-char non-hex should fail hex validation")
	}
}

// TestValidatePassword_DisabledCategories 全部关闭时仅校验最小长度（可配置为宽松策略）。
func TestValidatePassword_DisabledCategories(t *testing.T) {
	yaml := `
    min_length: 1
    require_upper: false
    require_lower: false
    require_digit: false
    require_special: false
    require_categories: 0
`
	svc := &AuthService{cfg: loadPasswordTestManager(t, yaml)}

	if err := svc.validatePassword("x"); err != nil {
		t.Fatalf("lenient policy should accept 'x', got %v", err)
	}
	if err := svc.validatePassword(""); err == nil {
		t.Fatal("empty password should fail min_length=1")
	}
}

// TestValidatePassword_RequireCategoriesThreshold 验证 require_categories 阈值语义：
// 仅满足 2 类但阈值为 2 时通过；阈值为 3 时不通过。
func TestValidatePassword_RequireCategoriesThreshold(t *testing.T) {
	base := `
    min_length: 4
    require_upper: true
    require_lower: true
    require_digit: true
    require_special: true
`
	// 阈值为 2：大写+小写 两类的 "ABCDefgh" 应通过。
	svc2 := &AuthService{cfg: loadPasswordTestManager(t, base+"    require_categories: 2\n")}
	if err := svc2.validatePassword("ABCDefgh"); err != nil {
		t.Fatalf("categories threshold=2 should pass 'ABCDefgh', got %v", err)
	}

	// 阈值为 3：同上仅 2 类应通过失败。
	svc3 := &AuthService{cfg: loadPasswordTestManager(t, base+"    require_categories: 3\n")}
	if err := svc3.validatePassword("ABCDefgh"); err == nil {
		t.Fatal("categories threshold=3 should reject 'ABCDefgh' (only 2 categories)")
	}
}
