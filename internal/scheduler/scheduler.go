// Package scheduler 轻量级定时任务调度器（零外部依赖，仅使用标准库 time/ticker）。
//
// 提供两类任务：
//   - AddIntervalJob：按固定间隔重复执行（如挂机超时扫描）。
//   - AddPeriodJob：按周期边界执行一次（如每日/每周任务进度重置），通过周期标识
//     （如 "2026-07-09"）变化触发，天然在周期边界运行，且启动时不会立即执行。
//
// 适用于单实例或主从分离场景；多实例部署时建议配合分布式锁/选主避免重复执行
// （本调度器本身不提供选主，结算逻辑已通过 DB 乐观锁做幂等保护）。
package scheduler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Logger 带级别的日志接口，避免把错误统一用 INFO 级别打印而淹没真实告警。
// *zap.SugaredLogger 天然满足该接口（具备 Infof/Warnf/Errorf 方法）。
type Logger interface {
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// noopLogger 空实现，SetLogger 未调用或传 nil 时使用，避免 nil 判空散落各处。
type noopLogger struct{}

func (noopLogger) Infof(string, ...interface{})  {}
func (noopLogger) Warnf(string, ...interface{})  {}
func (noopLogger) Errorf(string, ...interface{}) {}

// Job 定时任务函数
type Job func(ctx context.Context)

type scheduledJob struct {
	name     string
	kind     string // "interval" | "period"
	interval time.Duration
	poll     time.Duration
	periodFn func() string
	fn       Job
	lastKey  string
}

// Metrics 调度任务异常上报钩子，由宿主注入（默认 nil，即不上报）。
// 仅上报 panic（任务内非预期崩溃）；任务执行失败（闭包返回 error）由宿主在闭包内自行上报，
// 因 Job 签名无返回值、失败在闭包内消化，无法经此钩子统一捕获。
type Metrics interface {
	RecordPanic(name string)
}

// Scheduler 定时任务调度器
type Scheduler struct {
	mu          sync.Mutex
	jobs        []scheduledJob
	stopCh      chan struct{}
	wg          sync.WaitGroup
	log         Logger
	metricsHook Metrics // 任务 panic 上报钩子（nil 安全）
}

// New 创建调度器（默认日志丢弃）
func New() *Scheduler {
	return &Scheduler{stopCh: make(chan struct{}), log: noopLogger{}}
}

// SetLogger 设置带级别的日志（默认丢弃）。传 nil 等效于使用空日志。
func (s *Scheduler) SetLogger(l Logger) {
	if l == nil {
		l = noopLogger{}
	}
	s.log = l
}

// SetMetricsHook 注入任务 panic 上报钩子（可空，调用方传 nil 即关闭上报）。
func (s *Scheduler) SetMetricsHook(m Metrics) {
	s.metricsHook = m
}

// AddIntervalJob 按固定间隔运行任务（后台 ticker）。interval<=0 时该任务不会运行。
func (s *Scheduler) AddIntervalJob(name string, interval time.Duration, fn Job) {
	s.mu.Lock()
	s.jobs = append(s.jobs, scheduledJob{name: name, kind: "interval", interval: interval, fn: fn})
	s.mu.Unlock()
}

// AddPeriodJob 按周期边界运行一次。periodFn 返回当前周期标识（如 "2026-07-09"）；
// 当周期标识发生变化时才执行（天然在周期边界触发），且启动时不会立即执行。
// poll 为边界检测轮询间隔，<=0 时默认 1 分钟。
func (s *Scheduler) AddPeriodJob(name string, poll time.Duration, periodFn func() string, fn Job) {
	s.mu.Lock()
	s.jobs = append(s.jobs, scheduledJob{name: name, kind: "period", poll: poll, periodFn: periodFn, fn: fn})
	s.mu.Unlock()
}

// Start 启动所有任务（每个任务一个 goroutine）
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	jobs := make([]scheduledJob, len(s.jobs))
	copy(jobs, s.jobs)
	s.mu.Unlock()

	for i := range jobs {
		s.wg.Add(1)
		go s.run(ctx, &jobs[i])
	}
	s.log.Infof("[scheduler] started %d jobs", len(jobs))
}

func (s *Scheduler) run(ctx context.Context, j *scheduledJob) {
	defer s.wg.Done()
	switch j.kind {
	case "interval":
		s.runInterval(ctx, j)
	case "period":
		s.runPeriod(ctx, j)
	default:
		s.log.Errorf("[scheduler] unknown job kind: %s", j.kind)
	}
}

func (s *Scheduler) runInterval(ctx context.Context, j *scheduledJob) {
	if j.interval <= 0 {
		return
	}
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.exec(ctx, j)
		}
	}
}

func (s *Scheduler) runPeriod(ctx context.Context, j *scheduledJob) {
	poll := j.poll
	if poll <= 0 {
		poll = time.Minute
	}
	// 初始化为当前周期，避免启动时立即执行
	j.lastKey = j.periodFn()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			cur := j.periodFn()
			if cur != j.lastKey {
				j.lastKey = cur
				s.exec(ctx, j)
			}
		}
	}
}

// exec 执行任务并 recover panic，避免单个任务崩溃影响调度器
func (s *Scheduler) exec(ctx context.Context, j *scheduledJob) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Errorf("[scheduler] job %q panic recovered: %v", j.name, r)
			// panic 属非预期崩溃，上报计数供告警（与「消费 handler panic 拖垮进程」同源风险：
			// 持续 panic 应被运维经 metrics 发现，而非仅靠翻日志）。nil 钩子时不上报。
			if s.metricsHook != nil {
				s.metricsHook.RecordPanic(j.name)
			}
		}
	}()
	j.fn(ctx)
}

// Stop 停止所有任务并等待进行中的任务完成
func (s *Scheduler) Stop() {
	select {
	case <-s.stopCh:
		// 已关闭，避免重复 close panic
		return
	default:
	}
	close(s.stopCh)
	s.wg.Wait()
	s.log.Infof("[scheduler] stopped")
}

// ParseCronMinute 解析最简 cron 表达式的「分钟」字段，返回对应间隔。
// 支持："*"、"N"（第 N 分钟，等价于每 60 分钟）、"*/N"（每 N 分钟）。
// 其它格式返回 (0, false)。
func ParseCronMinute(expr string) (time.Duration, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return 0, false
	}
	fields := strings.Fields(expr)
	if len(fields) < 1 {
		return 0, false
	}
	minute := fields[0]
	switch {
	case minute == "*":
		return time.Minute, true
	case strings.HasPrefix(minute, "*/"):
		n := 0
		if _, err := fmt.Sscan(strings.TrimPrefix(minute, "*/"), &n); err == nil && n > 0 {
			return time.Duration(n) * time.Minute, true
		}
		return 0, false
	default:
		// 精确分钟值（如 "30"）按每 60 分钟处理
		n := 0
		if _, err := fmt.Sscan(minute, &n); err == nil && n >= 0 {
			return 60 * time.Minute, true
		}
		return 0, false
	}
}
