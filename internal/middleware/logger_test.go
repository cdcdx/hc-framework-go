package middleware

import (
	"encoding/json"
	"testing"
)

func defaultSensitive() map[string]bool {
	return map[string]bool{
		"password":      true,
		"token":         true,
		"secret":        true,
		"authorization": true,
	}
}

func TestMaskJSON_SensitiveFields(t *testing.T) {
	s := defaultSensitive()
	cases := []struct {
		name    string
		input   string
		wantKey string
		wantVal string
	}{
		{"password replaced", `{"password":"secret123","email":"a@b.com"}`, "password", "***"},
		{"token replaced", `{"token":"abc.def.ghi","user":"alice"}`, "token", "***"},
		{"secret replaced", `{"secret":"xyz","public":"data"}`, "secret", "***"},
		{"authorization replaced", `{"authorization":"Bearer xyz","id":1}`, "authorization", "***"},
		{"case-insensitive", `{"Password":"Secret123"}`, "Password", "***"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := maskJSON([]byte(c.input), s)
			var m map[string]interface{}
			if err := json.Unmarshal([]byte(got), &m); err != nil {
				t.Fatalf("unmarshal result: %v", err)
			}
			if m[c.wantKey] != c.wantVal {
				t.Errorf("key %q: got %v, want %v", c.wantKey, m[c.wantKey], c.wantVal)
			}
		})
	}
}

func TestMaskJSON_NonSensitiveUntouched(t *testing.T) {
	input := `{"name":"alice","age":30}`
	got := maskJSON([]byte(input), defaultSensitive())
	var m map[string]interface{}
	json.Unmarshal([]byte(got), &m)
	if m["name"] != "alice" {
		t.Error("non-sensitive key should not be masked")
	}
}

func TestMaskJSON_InvalidJSONReturnsOriginal(t *testing.T) {
	got := maskJSON([]byte(`not-json-at-all`), defaultSensitive())
	if got != "not-json-at-all" {
		t.Fatalf("expected original, got %s", got)
	}
}
