// Package trace 提供轻量、OpenTelemetry 兼容的链路追踪能力。
//
// 设计目标：
//   - 复用 OpenTelemetry 的 W3C Trace Context 传播标准（traceparent）与
//     otel 的 TraceID/SpanID 值类型，保证与 OTel 生态（Jaeger/Tempo）互通。
//   - 不依赖 otel/sdk 与 exporter 包（当前 go.mod 未引入），span 默认导出到
//     Zap 结构化日志，可直接在日志平台按 trace_id 串联。
//   - 通过替换 Provider 的 Exporter（实现 Exporter 接口并用 OTLP 导出器）即可
//     无缝切换到真实的 OTLP gRPC/HTTP 后端，业务代码无需改动。
package trace

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// ───────────────────────── Span 类型与状态 ─────────────────────────

// SpanKind 描述 Span 在调用链中的角色（与 OTel 语义一致）。
type SpanKind int

const (
	SpanKindInternal SpanKind = 1
	SpanKindServer   SpanKind = 2
	SpanKindClient   SpanKind = 3
	SpanKindProducer SpanKind = 4
	SpanKindConsumer SpanKind = 5
)

func (k SpanKind) String() string {
	switch k {
	case SpanKindServer:
		return "server"
	case SpanKindClient:
		return "client"
	case SpanKindProducer:
		return "producer"
	case SpanKindConsumer:
		return "consumer"
	default:
		return "internal"
	}
}

// StatusCode Span 状态（与 OTel 语义一致）。
type StatusCode int

const (
	StatusCodeUnset StatusCode = 0
	StatusCodeOK    StatusCode = 1
	StatusCodeError StatusCode = 2
)

func (c StatusCode) String() string {
	switch c {
	case StatusCodeOK:
		return "OK"
	case StatusCodeError:
		return "ERROR"
	default:
		return "UNSET"
	}
}

// SpanContext 携带一个 Span 的标识信息（使用 otel 的 TraceID/SpanID 类型）。
type SpanContext struct {
	TraceID    oteltrace.TraceID
	SpanID     oteltrace.SpanID
	TraceFlags oteltrace.TraceFlags
	Remote     bool
}

// IsValid 判断 SpanContext 是否合法（TraceID 与 SpanID 均非零）。
func (sc SpanContext) IsValid() bool {
	return sc.TraceID.IsValid() && sc.SpanID.IsValid()
}

// TraceIDHex 返回 32 位十六进制 TraceID。
func (sc SpanContext) TraceIDHex() string { return sc.TraceID.String() }

// SpanIDHex 返回 16 位十六进制 SpanID。
func (sc SpanContext) SpanIDHex() string { return sc.SpanID.String() }

// ───────────────────────── Span ─────────────────────────

// Span 表示一次操作（如一次 HTTP 请求、一次 DB 查询）的追踪单元。
type Span struct {
	mu         sync.Mutex
	name       string
	kind       SpanKind
	sc         SpanContext
	parent     SpanContext
	startTime  time.Time
	endTime    time.Time
	statusCode StatusCode
	statusDesc string
	err        error
	attributes map[string]string
	tracer     *Tracer
	ended      bool
}

// SpanContext 返回该 Span 的 SpanContext。
func (s *Span) SpanContext() SpanContext { return s.sc }

// TraceIDHex 返回所属 Trace 的十六进制 ID。
func (s *Span) TraceIDHex() string { return s.sc.TraceIDHex() }

// ParentIDHex 返回父 Span 的十六进制 ID（无父则返回空串）。
func (s *Span) ParentIDHex() string {
	if !s.parent.IsValid() {
		return ""
	}
	return s.parent.SpanIDHex()
}

// SetAttribute 设置单个属性（用于检索与过滤）。
func (s *Span) SetAttribute(key string, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attributes == nil {
		s.attributes = make(map[string]string)
	}
	s.attributes[key] = fmt.Sprintf("%v", value)
}

// SetAttributes 批量设置属性。
func (s *Span) SetAttributes(attrs map[string]interface{}) {
	for k, v := range attrs {
		s.SetAttribute(k, v)
	}
}

// AddEvent 记录一个时间点事件。
func (s *Span) AddEvent(name string) {
	s.SetAttribute("event:"+name, time.Now().Format(time.RFC3339Nano))
}

// SetStatus 设置 Span 状态与描述。
func (s *Span) SetStatus(code StatusCode, desc string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusCode = code
	s.statusDesc = desc
}

// RecordError 记录错误并将状态置为 ERROR。
func (s *Span) RecordError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
	s.statusCode = StatusCodeError
}

// End 结束 Span 并触发导出。可安全重复调用（仅首次生效）。
func (s *Span) End() {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	s.endTime = time.Now()
	rec := spanRecord{
		name:       s.name,
		kind:       s.kind.String(),
		sc:         s.sc,
		parent:     s.parent,
		startTime:  s.startTime,
		endTime:    s.endTime,
		duration:   s.endTime.Sub(s.startTime),
		status:     s.statusCode.String(),
		statusDesc: s.statusDesc,
		err:        s.err,
		attributes: s.attributes,
	}
	t := s.tracer
	s.mu.Unlock()

	if t != nil && t.provider != nil {
		rec.service = t.provider.serviceName
		t.provider.export(rec)
	}
}

// spanRecord 是 Export 时传递的不可变快照。
type spanRecord struct {
	service    string
	name       string
	kind       string
	sc         SpanContext
	parent     SpanContext
	startTime  time.Time
	endTime    time.Time
	duration   time.Duration
	status     string
	statusDesc string
	err        error
	attributes map[string]string
}

// ───────────────────────── Tracer / Provider ─────────────────────────

// Tracer 创建 Span 的构造器。
type Tracer struct {
	provider *Provider
	name     string
}

// SpanOption 用于配置 Span 的启动选项。
type SpanOption func(*Span)

// WithKind 设置 Span 角色。
func WithKind(kind SpanKind) SpanOption {
	return func(s *Span) { s.kind = kind }
}

// WithAttributes 设置 Span 初始属性。
func WithAttributes(attrs map[string]interface{}) SpanOption {
	return func(s *Span) { s.SetAttributes(attrs) }
}

// Start 创建并开始一个 Span，返回携带该 Span 的新 context。
func (t *Tracer) Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, *Span) {
	parent := SpanFromContext(ctx)
	var psc SpanContext
	if parent != nil {
		psc = parent.sc
	} else if osc := oteltrace.SpanContextFromContext(ctx); osc.IsValid() {
		// 来自上游 W3C traceparent 传播的远程父上下文
		psc = SpanContext{
			TraceID:    osc.TraceID(),
			SpanID:     osc.SpanID(),
			TraceFlags: osc.TraceFlags(),
			Remote:     osc.IsRemote(),
		}
	}

	var sc SpanContext
	if psc.IsValid() {
		// 继承父 TraceID，生成本 Span 的新 SpanID
		sc = SpanContext{TraceID: psc.TraceID, SpanID: newSpanID(), TraceFlags: oteltrace.FlagsSampled}
	} else {
		// 根 Span：生成新的 TraceID
		sc = SpanContext{TraceID: newTraceID(), SpanID: newSpanID(), TraceFlags: oteltrace.FlagsSampled}
	}

	span := &Span{
		name:      name,
		kind:      SpanKindInternal,
		sc:        sc,
		parent:    psc,
		startTime: time.Now(),
		tracer:    t,
	}
	for _, o := range opts {
		o(span)
	}

	// 将 otel SpanContext 写入 ctx，供 W3C Inject 向下游传播
	otelSC := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    sc.TraceID,
		SpanID:     sc.SpanID,
		TraceFlags: sc.TraceFlags,
		Remote:     false,
	})
	newCtx := oteltrace.ContextWithSpanContext(ctx, otelSC)
	newCtx = context.WithValue(newCtx, spanKey, span)
	return newCtx, span
}

// Provider 持有全局 Tracer 与导出器。
type Provider struct {
	serviceName string
	tracerName  string
	exporter    Exporter
	sampler     samplerState
}

// NewProvider 创建追踪 Provider。exporter 为 nil 时默认使用日志导出器（写入传入 logger）。
func NewProvider(serviceName string, logger *zap.Logger, exporter Exporter) *Provider {
	if serviceName == "" {
		serviceName = "hc-framework"
	}
	if exporter == nil {
		exporter = NewLogExporter(logger)
	}
	return &Provider{
		serviceName: serviceName,
		tracerName:  "hc-framework",
		exporter:    exporter,
		sampler:     samplerState{ratio: -1}, // 默认全采样
	}
}

// Tracer 返回命名 Tracer。
func (p *Provider) Tracer(name string) *Tracer {
	if name == "" {
		name = p.tracerName
	}
	return &Tracer{provider: p, name: name}
}

// ServiceName 返回服务名（用于导出时的资源标识）。
func (p *Provider) ServiceName() string { return p.serviceName }

func (p *Provider) export(rec spanRecord) {
	if p.exporter == nil {
		return
	}
	if !p.shouldExport(rec.sc, rec.err != nil) {
		return
	}
	p.exporter.Export(context.Background(), rec)
}

// ───────────────────────── Exporter ─────────────────────────

// Exporter 导出已结束 Span 的接口。实现该接口即可对接任意后端（日志、OTLP 等）。
type Exporter interface {
	Export(ctx context.Context, rec spanRecord)
}

// LogExporter 将 Span 以结构化日志形式输出（默认导出器）。
type LogExporter struct {
	logger *zap.Logger
}

// NewLogExporter 创建日志导出器。
func NewLogExporter(logger *zap.Logger) *LogExporter {
	return &LogExporter{logger: logger}
}

// Export 输出一条 span 日志。
func (e *LogExporter) Export(_ context.Context, rec spanRecord) {
	if e.logger == nil {
		return
	}
	fields := []zap.Field{
		zap.String("service", rec.service),
		zap.String("trace_id", rec.sc.TraceIDHex()),
		zap.String("span_id", rec.sc.SpanIDHex()),
		zap.String("parent_id", rec.parent.SpanIDHex()),
		zap.String("name", rec.name),
		zap.String("kind", rec.kind),
		zap.Int64("duration_ms", rec.duration.Milliseconds()),
		zap.String("status", rec.status),
	}
	if rec.statusDesc != "" {
		fields = append(fields, zap.String("status_desc", rec.statusDesc))
	}
	if rec.err != nil {
		fields = append(fields, zap.String("error", rec.err.Error()))
	}
	for k, v := range rec.attributes {
		fields = append(fields, zap.String("attr."+k, v))
	}
	e.logger.Info("span", fields...)
}

// ───────────────────────── 全局 / context 辅助 ─────────────────────────

type spanKeyType struct{}

var spanKey = spanKeyType{}

// SpanFromContext 从 context 中取出当前 Span（无则返回 nil）。
func SpanFromContext(ctx context.Context) *Span {
	if s, ok := ctx.Value(spanKey).(*Span); ok {
		return s
	}
	return nil
}

var (
	defaultMu   sync.RWMutex
	defaultProv *Provider
)

// SetGlobal 注册全局 Provider（应在 main 初始化时调用一次）。
func SetGlobal(p *Provider) {
	defaultMu.Lock()
	defaultProv = p
	defaultMu.Unlock()
}

// InitProvider 便捷初始化：创建 Provider 并注册为全局，返回该 Provider 实例。
// exporter 传 nil 时使用默认日志导出器。
func InitProvider(serviceName string, logger *zap.Logger) *Provider {
	p := NewProvider(serviceName, logger, nil)
	SetGlobal(p)
	return p
}

// Global 返回全局 Provider。
func Global() *Provider {
	defaultMu.RLock()
	defer defaultMu.RUnlock()
	return defaultProv
}

// DefaultTracer 返回全局默认 Tracer（未初始化时返回安全的 no-op Tracer）。
func DefaultTracer() *Tracer {
	p := Global()
	if p == nil {
		return &Tracer{provider: &Provider{serviceName: "hc-framework"}}
	}
	return p.Tracer("hc-framework")
}

// Start 便捷方法：使用全局 Tracer 开启一个 Span。
func Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, *Span) {
	return DefaultTracer().Start(ctx, name, opts...)
}

// ───────────────────────── W3C 传播（HTTP） ─────────────────────────

var propagator = propagation.TraceContext{}

// HeaderTraceID 旧版链路追踪使用的请求/响应头（兼容 X-Trace-Id）。
const HeaderTraceID = "X-Trace-Id"

// getCarrier 只读 HeaderCarrier 适配，满足 otel propagation.TextMapCarrier。
type getCarrier struct {
	get func(string) string
}

func (c getCarrier) Get(key string) string { return c.get(key) }
func (c getCarrier) Set(string, string)    {}
func (c getCarrier) Keys() []string        { return nil }

// ExtractHTTPHeader 从 HTTP 请求头提取 W3C traceparent 构造父上下文。
// 若缺失但存在旧版 X-Trace-Id，则以其为远程父上下文，保证与旧接入方兼容。
func ExtractHTTPHeader(ctx context.Context, get func(string) string) context.Context {
	extracted := propagator.Extract(ctx, getCarrier{get: get})
	if oteltrace.SpanContextFromContext(extracted).IsValid() {
		return extracted
	}
	if old := get(HeaderTraceID); old != "" {
		if tid, err := oteltrace.TraceIDFromHex(old); err == nil && tid.IsValid() {
			osc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
				TraceID:    tid,
				SpanID:     newSpanID(),
				TraceFlags: oteltrace.FlagsSampled,
				Remote:     true,
			})
			return oteltrace.ContextWithRemoteSpanContext(ctx, osc)
		}
	}
	return ctx
}

// ───────────────────────── 内部工具 ─────────────────────────

func newTraceID() oteltrace.TraceID {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return oteltrace.TraceID(b)
}

func newSpanID() oteltrace.SpanID {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return oteltrace.SpanID(b)
}
