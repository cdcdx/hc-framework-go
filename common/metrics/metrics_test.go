package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestFlashRedeemTotal 验证计数器单调递增。
func TestFlashRedeemTotal(t *testing.T) {
	before := testutil.ToFloat64(FlashRedeemTotal)
	FlashRedeemTotal.Inc()
	after := testutil.ToFloat64(FlashRedeemTotal)
	if after != before+1 {
		t.Fatalf("FlashRedeemTotal expected %v got %v", before+1, after)
	}
}

// TestFlashRedeemTimeout_Labeled 验证 reason 维度的计数器可独立累加。
func TestFlashRedeemTimeout_Labeled(t *testing.T) {
	cases := []string{"db_slow", "downstream", "context_deadline"}
	for _, reason := range cases {
		before := testutil.ToFloat64(FlashRedeemTimeout.WithLabelValues(reason))
		FlashRedeemTimeout.WithLabelValues(reason).Inc()
		after := testutil.ToFloat64(FlashRedeemTimeout.WithLabelValues(reason))
		if after != before+1 {
			t.Fatalf("FlashRedeemTimeout[%s] expected %v got %v", reason, before+1, after)
		}
	}
}

// TestDBPoolMetrics 验证连接池指标注册到默认 registry 且可写入。
func TestDBPoolMetrics(t *testing.T) {
	DBPoolUtilization.WithLabelValues("main").Set(0.5)
	DBPoolWaitCount.WithLabelValues("main").Inc()

	// 确认指标确实注册在 default registry 中（promhttp 暴露的根）。
	metricFamilies, err := prometheus.DefaultRegisterer.(prometheus.Gatherer).Gather()
	if err != nil {
		t.Fatalf("gather default registry: %v", err)
	}
	want := map[string]bool{
		"db_pool_utilization":        false,
		"db_pool_wait_count_total":   false,
		"shop_flash_redeem_total":    false,
		"idle_scan_duration_seconds": false,
	}
	for _, mf := range metricFamilies {
		if _, ok := want[mf.GetName()]; ok {
			want[mf.GetName()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("expected metric %q registered in default registry", name)
		}
	}
}

// TestIdleScanDurationObserve 验证 Histogram 可观测且不 panic。
func TestIdleScanDurationObserve(t *testing.T) {
	IdleScanDurationSeconds.Observe(0.12)
	IdleScanDurationSeconds.Observe(0.34)
	// 仅验证多次观测可正常累积（无 panic 即视为通过）。
	count := testutil.CollectAndCount(IdleScanDurationSeconds)
	if count == 0 {
		t.Fatalf("expected idle_scan_duration_seconds to be collected")
	}
}
