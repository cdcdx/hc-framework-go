package trace

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/metrics"
)

// ───────────────────────── OTLP/HTTP JSON 导出器 ─────────────────────────
//
// 为满足「不引入 otel/sdk 与 exporter 重型依赖」的约束，这里手搓 OTLP/HTTP JSON
// 协议（见 OpenTelemetry 规范的 OTLP/HTTP 章节）：将 span 以 JSON 批量 POST 到
// collector 的 /v1/traces 端点。可直接对接 Tempo、Jaeger（均支持 OTLP/HTTP）。
// 业务代码（中间件 / 服务）无需任何改动。

// OTLP/HTTP JSON 协议结构（仅保留我们用到的字段）。
type otlpValue struct {
	StringValue string `json:"stringValue,omitempty"`
}

type otlpKeyValue struct {
	Key   string    `json:"key"`
	Value otlpValue `json:"value"`
}

type otlpStatus struct {
	Code    int    `json:"code"` // 0=UNSET 1=OK 2=ERROR
	Message string `json:"message,omitempty"`
}

type otlpSpan struct {
	TraceID           string         `json:"traceId"`
	SpanID            string         `json:"spanId"`
	ParentSpanID      string         `json:"parentSpanId,omitempty"`
	Name              string         `json:"name"`
	Kind              int            `json:"kind"` // 1=INTERNAL 2=SERVER 3=CLIENT 4=PRODUCER 5=CONSUMER
	StartTimeUnixNano string         `json:"startTimeUnixNano"`
	EndTimeUnixNano   string         `json:"endTimeUnixNano"`
	Attributes        []otlpKeyValue `json:"attributes,omitempty"`
	Status            otlpStatus     `json:"status"`
}

type otlpScopeSpans struct {
	Scope otlpScope  `json:"scope"`
	Spans []otlpSpan `json:"spans"`
}

type otlpScope struct {
	Name string `json:"name"`
}

type otlpResourceSpans struct {
	Resource   otlpResource     `json:"resource"`
	ScopeSpans []otlpScopeSpans `json:"scopeSpans"`
}

type otlpResource struct {
	Attributes []otlpKeyValue `json:"attributes"`
}

type otlpTraces struct {
	ResourceSpans []otlpResourceSpans `json:"resourceSpans"`
}

// OTLPHTTPExporter 将 span 通过 OTLP/HTTP JSON 发送到 collector。
// 内部以「有缓冲 channel + 后台单 worker 批量 flush」方式发送，避免阻塞业务请求。
type OTLPHTTPExporter struct {
	baseURL     string // 完整 URL，如 http://127.0.0.1:4318/v1/traces
	client      *http.Client
	headers     map[string]string
	serviceName string
	scopeName   string
	insecure    bool

	ch        chan spanRecord
	batchSize int
	flushInt  time.Duration
	dropped   int64

	closeOnce sync.Once
	done      chan struct{}
	wg        sync.WaitGroup
}

// OTLPOption 配置 OTLP 导出器。
type OTLPOption func(*OTLPHTTPExporter)

// WithInsecure 当 endpoint 未带 scheme 时，使用 http（而非默认 https）。
func WithInsecure(insecure bool) OTLPOption {
	return func(e *OTLPHTTPExporter) { e.insecure = insecure }
}

// WithHeaders 设置额外请求头（如鉴权）。
func WithHeaders(h map[string]string) OTLPOption {
	return func(e *OTLPHTTPExporter) {
		for k, v := range h {
			e.headers[k] = v
		}
	}
}

// WithTimeout 设置单次 HTTP 请求超时。
func WithTimeout(d time.Duration) OTLPOption {
	return func(e *OTLPHTTPExporter) { e.client.Timeout = d }
}

// WithServiceName 设置 resource 的 service.name。
func WithServiceName(name string) OTLPOption {
	return func(e *OTLPHTTPExporter) {
		if name != "" {
			e.serviceName = name
		}
	}
}

// WithScopeName 设置 scope 名（默认 hc-framework）。
func WithScopeName(name string) OTLPOption {
	return func(e *OTLPHTTPExporter) {
		if name != "" {
			e.scopeName = name
		}
	}
}

// WithBatch 设置批量参数：单批最大 span 数与定flush 间隔。
func WithBatch(size int, interval time.Duration) OTLPOption {
	return func(e *OTLPHTTPExporter) {
		if size > 0 {
			e.batchSize = size
		}
		if interval > 0 {
			e.flushInt = interval
		}
	}
}

// NewOTLPHTTPExporter 创建 OTLP/HTTP 导出器并启动后台发送 worker。
// endpoint 可为 "host:port" 或完整 URL；未带 scheme 时按 insecure 决定 http/https。
func NewOTLPHTTPExporter(endpoint string, opts ...OTLPOption) *OTLPHTTPExporter {
	e := &OTLPHTTPExporter{
		serviceName: "hc-framework",
		scopeName:   "hc-framework",
		headers:     map[string]string{"Content-Type": "application/json"},
		client:      &http.Client{Timeout: 5 * time.Second},
		ch:          make(chan spanRecord, 2048),
		batchSize:   512,
		flushInt:    2 * time.Second,
		done:        make(chan struct{}),
		insecure:    true, // 默认内网 collector 走 http
	}
	for _, o := range opts {
		o(e)
	}

	base := endpoint
	if !strings.Contains(base, "://") {
		scheme := "https"
		if e.insecure {
			scheme = "http"
		}
		base = scheme + "://" + base
	}
	e.baseURL = strings.TrimRight(base, "/") + "/v1/traces"

	e.wg.Add(1)
	go e.worker()
	return e
}

// Export 将 span 入队（非阻塞；队列满则丢弃并计数）。
func (e *OTLPHTTPExporter) Export(_ context.Context, rec spanRecord) {
	select {
	case e.ch <- rec:
	default:
		// 队列满：丢弃 span 并计数（进程内 + Prometheus 双口径）。
		// Prometheus 计数便于配置「丢 span」告警，弥补进程内 dropped 只能 scrape 不到的盲区。
		atomic.AddInt64(&e.dropped, 1)
		metrics.TraceSpansDroppedTotal.Inc()
	}
}

// Dropped 返回因队列满被丢弃的 span 数（用于观测）。
func (e *OTLPHTTPExporter) Dropped() int64 { return atomic.LoadInt64(&e.dropped) }

// Shutdown 优雅关闭：停止接收、flush 剩余 span，等待 worker 退出。
func (e *OTLPHTTPExporter) Shutdown(ctx context.Context) error {
	e.closeOnce.Do(func() { close(e.done) })
	stopped := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *OTLPHTTPExporter) worker() {
	defer e.wg.Done()
	batch := make([]spanRecord, 0, e.batchSize)
	ticker := time.NewTicker(e.flushInt)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		e.send(batch)
		batch = batch[:0]
	}

	for {
		select {
		case rec := <-e.ch:
			batch = append(batch, rec)
			if len(batch) >= e.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-e.done:
			// 排空队列后退出
			for {
				select {
				case rec := <-e.ch:
					batch = append(batch, rec)
				default:
					flush()
					return
				}
			}
		}
	}
}

// send 将一批 span 序列化为 OTLP JSON 并 POST。
func (e *OTLPHTTPExporter) send(records []spanRecord) {
	spans := make([]otlpSpan, 0, len(records))
	for _, rec := range records {
		s := otlpSpan{
			TraceID:           rec.sc.TraceIDHex(),
			SpanID:            rec.sc.SpanIDHex(),
			ParentSpanID:      rec.parent.SpanIDHex(),
			Name:              rec.name,
			Kind:              spanKindToInt(rec.kind),
			StartTimeUnixNano: strconv.FormatInt(rec.startTime.UnixNano(), 10),
			EndTimeUnixNano:   strconv.FormatInt(rec.endTime.UnixNano(), 10),
			Status: otlpStatus{
				Code:    spanStatusToInt(rec.status),
				Message: rec.statusDesc,
			},
		}
		for k, v := range rec.attributes {
			s.Attributes = append(s.Attributes, otlpKeyValue{
				Key:   k,
				Value: otlpValue{StringValue: v},
			})
		}
		spans = append(spans, s)
	}

	payload := otlpTraces{
		ResourceSpans: []otlpResourceSpans{
			{
				Resource: otlpResource{
					Attributes: []otlpKeyValue{
						{Key: "service.name", Value: otlpValue{StringValue: e.serviceName}},
					},
				},
				ScopeSpans: []otlpScopeSpans{
					{
						Scope: otlpScope{Name: e.scopeName},
						Spans: spans,
					},
				},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return
	}

	req, err := http.NewRequest(http.MethodPost, e.baseURL, bytes.NewReader(body))
	if err != nil {
		return
	}
	for k, v := range e.headers {
		req.Header.Set(k, v)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		// collector 不可用时静默丢弃（链路追踪不应影响主流程）
		return
	}
	defer resp.Body.Close()
}

func spanKindToInt(k string) int {
	switch k {
	case "server":
		return 2
	case "client":
		return 3
	case "producer":
		return 4
	case "consumer":
		return 5
	default:
		return 1
	}
}

func spanStatusToInt(s string) int {
	switch s {
	case "OK":
		return 1
	case "ERROR":
		return 2
	default:
		return 0
	}
}

// ───────────────────────── Noop 导出器 ─────────────────────────

// NoopExporter 丢弃所有 span（exporter=none 时使用）。
type NoopExporter struct{}

// NewNoopExporter 创建空导出器。
func NewNoopExporter() *NoopExporter { return &NoopExporter{} }

// Export 空实现。
func (NoopExporter) Export(_ context.Context, _ spanRecord) {}

// ───────────────────────── 采样配置 ─────────────────────────

// SamplerConfig 描述采样策略（与 config.TracingConfig.Sampler 对应）。
type SamplerConfig struct {
	Type      string  // always_on / always_off / traceidratio / parentbased_traceidratio
	Rate      float64 // 采样率 0~1
	ErrorRate float64 // 错误采样率 0~1
}

type samplerState struct {
	alwaysOff   bool
	ratio       float64 // <0 表示 always on
	errorAlways bool
}

// SetSampler 根据配置设置采样策略。
func (p *Provider) SetSampler(cfg SamplerConfig) {
	switch strings.ToLower(cfg.Type) {
	case "always_off":
		p.sampler = samplerState{alwaysOff: true}
	case "always_on":
		p.sampler = samplerState{ratio: -1}
	default: // traceidratio / parentbased_traceidratio
		p.sampler = samplerState{ratio: cfg.Rate, errorAlways: cfg.ErrorRate >= 1.0}
	}
}

// shouldExport 依据采样策略与 traceID 决定是否导出该 span。
// 同一 trace 的所有 span 共享 traceID，故采样决策天然一致。
func (p *Provider) shouldExport(sc SpanContext, isErr bool) bool {
	s := p.sampler
	if s.alwaysOff {
		return false
	}
	if isErr && s.errorAlways {
		return true
	}
	if s.ratio < 0 {
		return true
	}
	if !sc.IsValid() {
		return true
	}
	last := sc.TraceID[15]
	return float64(last)/255.0 < s.ratio
}

// Shutdown 优雅关闭 Provider（转发给支持 Shutdown 的 Exporter）。
func (p *Provider) Shutdown(ctx context.Context) error {
	if s, ok := p.exporter.(interface {
		Shutdown(context.Context) error
	}); ok {
		return s.Shutdown(ctx)
	}
	return nil
}
