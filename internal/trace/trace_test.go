package trace

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/cdcdx/hc-framework-go/internal/metrics"
)

// captureExporter 捕获导出的 span 记录，供断言。
type captureExporter struct {
	mu   sync.Mutex
	recs []spanRecord
}

func (e *captureExporter) Export(_ context.Context, rec spanRecord) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.recs = append(e.recs, rec)
}

func (e *captureExporter) last() spanRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.recs) == 0 {
		return spanRecord{}
	}
	return e.recs[len(e.recs)-1]
}

func (e *captureExporter) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.recs)
}

func newTestProvider(exp Exporter) *Provider {
	p := NewProvider("test-svc", nil, exp)
	p.SetSampler(SamplerConfig{Type: "always_on"})
	return p
}

// TestSpan_EndExports 验证 Span.End 触发导出，并携带属性/状态/错误。
func TestSpan_EndExports(t *testing.T) {
	exp := &captureExporter{}
	p := newTestProvider(exp)
	_, span := p.Tracer("t").Start(context.Background(), "op")
	span.SetAttribute("k", "v")
	span.RecordError(errors.New("boom"))
	span.End()

	if exp.count() != 1 {
		t.Fatalf("exported %d spans, want 1", exp.count())
	}
	r := exp.last()
	if r.name != "op" {
		t.Fatalf("name = %q, want op", r.name)
	}
	if r.status != "ERROR" {
		t.Fatalf("status = %q, want ERROR", r.status)
	}
	if r.attributes["k"] != "v" {
		t.Fatalf("attr k = %q, want v", r.attributes["k"])
	}
	if r.service != "test-svc" {
		t.Fatalf("service = %q, want test-svc", r.service)
	}
}

// TestSpan_ParentChildTraceID 验证子 Span 继承父 TraceID，且 parent 指向父 SpanID。
func TestSpan_ParentChildTraceID(t *testing.T) {
	exp := &captureExporter{}
	p := newTestProvider(exp)
	_, parent := p.Tracer("t").Start(context.Background(), "parent")
	childCtx := context.WithValue(context.Background(), spanKey, parent)
	_, child := p.Tracer("t").Start(childCtx, "child")

	if parent.TraceIDHex() != child.TraceIDHex() {
		t.Fatal("child should inherit parent trace id")
	}
	if child.ParentIDHex() != parent.SpanContext().SpanIDHex() {
		t.Fatalf("child parent = %q, want parent span id %q", child.ParentIDHex(), parent.SpanContext().SpanIDHex())
	}
	parent.End()
	child.End()
}

// TestSpan_EndIdempotent 验证重复 End 仅导出一次。
func TestSpan_EndIdempotent(t *testing.T) {
	exp := &captureExporter{}
	p := newTestProvider(exp)
	_, span := p.Tracer("t").Start(context.Background(), "op")
	span.End()
	span.End()
	if exp.count() != 1 {
		t.Fatalf("double End exported %d spans, want 1", exp.count())
	}
}

// TestExtractHTTPHeader_W3C 验证从 W3C traceparent 提取远程父上下文。
func TestExtractHTTPHeader_W3C(t *testing.T) {
	tid := "4bf92f3577b34da6a3ce929d0e0e4736"
	sid := "00f067aa0ba902b7"
	tp := "00-" + tid + "-" + sid + "-01"
	ctx := ExtractHTTPHeader(context.Background(), func(k string) string {
		if k == "traceparent" {
			return tp
		}
		return ""
	})
	sc := oteltrace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		t.Fatal("should extract valid W3C span context")
	}
	if sc.TraceID().String() != tid {
		t.Fatalf("trace id = %s, want %s", sc.TraceID().String(), tid)
	}
	if !sc.IsRemote() {
		t.Fatal("extracted context should be remote")
	}
}

// TestExtractHTTPHeader_LegacyXTraceID 验证旧版 X-Trace-Id 兜底为远程父上下文。
func TestExtractHTTPHeader_LegacyXTraceID(t *testing.T) {
	tid := "4bf92f3577b34da6a3ce929d0e0e4736"
	ctx := ExtractHTTPHeader(context.Background(), func(k string) string {
		if k == HeaderTraceID {
			return tid
		}
		return ""
	})
	sc := oteltrace.SpanContextFromContext(ctx)
	if !sc.IsValid() || !sc.IsRemote() {
		t.Fatal("legacy X-Trace-Id should set a valid remote parent")
	}
	if sc.TraceID().String() != tid {
		t.Fatalf("trace id = %s, want %s", sc.TraceID().String(), tid)
	}
}

// TestProvider_SamplerAlwaysOff 验证 always_off 不导出。
func TestProvider_SamplerAlwaysOff(t *testing.T) {
	exp := &captureExporter{}
	p := NewProvider("s", nil, exp)
	p.SetSampler(SamplerConfig{Type: "always_off"})
	_, span := p.Tracer("t").Start(context.Background(), "op")
	span.End()
	if exp.count() != 0 {
		t.Fatalf("always_off exported %d spans, want 0", exp.count())
	}
}

// TestProvider_SamplerRatioZero 验证 ratio=0 丢弃（非错误样本）。
func TestProvider_SamplerRatioZero(t *testing.T) {
	exp := &captureExporter{}
	p := NewProvider("s", nil, exp)
	p.SetSampler(SamplerConfig{Type: "traceidratio", Rate: 0})
	_, span := p.Tracer("t").Start(context.Background(), "op")
	span.End()
	if exp.count() != 0 {
		t.Fatalf("ratio=0 exported %d spans, want 0", exp.count())
	}
}

// TestOTLPExporter_DropIncrementsMetric 守护「OTLP 队列满丢弃时同步上报 Prometheus 计数」：
// Export 在 channel 满的分支既更新进程内 dropped，也 Inc trace_spans_dropped_total，
// 弥补进程内计数 scrape 不到的盲区（collector 不可达/背压丢 span 时运维可经告警发现）。
func TestOTLPExporter_DropIncrementsMetric(t *testing.T) {
	e := &OTLPHTTPExporter{ch: make(chan spanRecord, 1)}
	e.ch <- spanRecord{name: "fill"} // 占满唯一的缓冲槽

	before := testutil.ToFloat64(metrics.TraceSpansDroppedTotal)
	e.Export(context.Background(), spanRecord{name: "dropped"})
	after := testutil.ToFloat64(metrics.TraceSpansDroppedTotal)

	if after-before != 1 {
		t.Fatalf("TraceSpansDroppedTotal delta = %v, want 1", after-before)
	}
	if e.Dropped() != 1 {
		t.Fatalf("exporter.Dropped() = %d, want 1", e.Dropped())
	}
}
