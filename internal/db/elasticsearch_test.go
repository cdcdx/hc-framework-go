package db

import "testing"

// TestElasticsearchAdapter_Close_NoConnection 验证未 Connect（client 为 nil）时
// Close 的幂等性：第一次与重复调用均返回 nil，不触碰网络，也不 panic。
func TestElasticsearchAdapter_Close_NoConnection(t *testing.T) {
	a := NewElasticsearchAdapter(ElasticsearchCfg{
		Addresses:   []string{"http://localhost:9999"},
		IndexPrefix: "hc_test",
	})

	if err := a.Close(); err != nil {
		t.Fatalf("Close on unconnected adapter returned error: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("repeated Close on unconnected adapter returned error: %v", err)
	}
}
