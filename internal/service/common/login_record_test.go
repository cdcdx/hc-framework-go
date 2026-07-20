package common

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/model"
)

// fakeLogRepo 内存实现，用于验证合并后的 LogLogin 写入（audit_logs 含登录字段）。
// 仅实现 LogRepo 接口（不实现 BatchLogRepo），使 LogService 回退到逐条 Create，便于精确计数。
type fakeLogRepo struct {
	mu      sync.Mutex
	logs    []*model.AuditLog
	metrics []*model.MonitorMetric
}

func (f *fakeLogRepo) Create(_ context.Context, log *model.AuditLog) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs = append(f.logs, log)
	return nil
}
func (f *fakeLogRepo) FindByUser(context.Context, string, int64, int) ([]model.AuditLog, error) {
	return nil, nil
}
func (f *fakeLogRepo) FindByType(context.Context, string, time.Time, time.Time, int) ([]model.AuditLog, error) {
	return nil, nil
}
func (f *fakeLogRepo) CountByType(context.Context, string, time.Time, time.Time) (int64, error) {
	return 0, nil
}
func (f *fakeLogRepo) CountByTypeAndResult(_ context.Context, eventType, result string, start, end time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	for _, l := range f.logs {
		if l.EventType != eventType {
			continue
		}
		if result != "" && l.LoginResult != result {
			continue
		}
		n++
	}
	return n, nil
}
func (f *fakeLogRepo) SQLDB() (*sql.DB, error) { return nil, nil }
func (f *fakeLogRepo) Close() error            { return nil }

// 以下为 MonitorRepo 实现，用于捕获 writeMetric 写入的指标（成功 login_count / 失败 login_fail_count）。
func (f *fakeLogRepo) Record(_ context.Context, m *model.MonitorMetric) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.metrics = append(f.metrics, m)
	return nil
}
func (f *fakeLogRepo) RecordBatch(ctx context.Context, ms []*model.MonitorMetric) error {
	for _, m := range ms {
		if err := f.Record(ctx, m); err != nil {
			return err
		}
	}
	return nil
}
func (f *fakeLogRepo) SumByType(_ context.Context, metricType string, _, _ time.Time) (float64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var sum float64
	for _, m := range f.metrics {
		if m.MetricType == metricType {
			sum += m.Value
		}
	}
	return sum, nil
}
func (f *fakeLogRepo) FindByTimeRange(context.Context, time.Time, time.Time, int) ([]model.MonitorMetric, error) {
	return nil, nil
}

func (f *fakeLogRepo) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.logs)
}

// TestLogService_LogLogin_Merged 验证合并后登录审计与原审计日志同落 audit_logs 一行，
// 且登录专属字段被正确填充（原 login_records 表已移除，登录写入从 2 次降到 1 次）。
func TestLogService_LogLogin_Merged(t *testing.T) {
	fake := &fakeLogRepo{}
	s := NewLogService(fake, nil)
	s.LogLogin(context.Background(), EventMeta{UserID: "u1", IPAddress: "1.2.3.4"},
		model.LoginTypePassword, model.LoginResultSuccess, "u1@example.com", "", "iPhone")
	// Close 触发 logLoop 排空并 flush 剩余缓冲（确定性，无需等待 ticker）。
	s.Close()

	if got := fake.count(); got != 1 {
		t.Fatalf("audit logs written = %d, want 1", got)
	}
	rec := fake.logs[0]
	if rec.EventType != "login" {
		t.Fatalf("event_type = %q, want login", rec.EventType)
	}
	if rec.LoginType != model.LoginTypePassword {
		t.Fatalf("login_type = %q, want %q", rec.LoginType, model.LoginTypePassword)
	}
	if rec.LoginResult != model.LoginResultSuccess {
		t.Fatalf("login_result = %q, want %q", rec.LoginResult, model.LoginResultSuccess)
	}
	if rec.DeviceInfo != "iPhone" {
		t.Fatalf("device_info = %q, want iPhone", rec.DeviceInfo)
	}
	if rec.UserID != "u1" || rec.IPAddress != "1.2.3.4" {
		t.Fatalf("user_id=%q ip=%q, want u1/1.2.3.4", rec.UserID, rec.IPAddress)
	}
}

// TestLogService_LogLogin_FailRecorded 验证失败登录同样落 audit_logs（合并前失败登录只落
// login_records），且 login_result='fail'、fail_reason 被正确填充，登录写入仍只有 1 行。
func TestLogService_LogLogin_FailRecorded(t *testing.T) {
	fake := &fakeLogRepo{}
	s := NewLogService(fake, nil)
	s.LogLogin(context.Background(), EventMeta{UserID: "u2", IPAddress: "9.9.9.9"},
		model.LoginTypePassword, model.LoginResultFail, "u2@example.com", model.FailReasonPasswordWrong, "")
	s.Close()

	if got := fake.count(); got != 1 {
		t.Fatalf("audit logs written = %d, want 1 (失败登录也应只有 1 行)", got)
	}
	rec := fake.logs[0]
	if rec.EventType != "login" {
		t.Fatalf("event_type = %q, want login", rec.EventType)
	}
	if rec.LoginResult != model.LoginResultFail {
		t.Fatalf("login_result = %q, want %q", rec.LoginResult, model.LoginResultFail)
	}
	if rec.FailReason != model.FailReasonPasswordWrong {
		t.Fatalf("fail_reason = %q, want %q", rec.FailReason, model.FailReasonPasswordWrong)
	}
	// 失败登录现会写 login_fail_count 指标（仅当 monitorRepo 已注入）；本测试 monitorRepo 为 nil，
	// writeMetric 静默 no-op，故此处仅确认审计行本身已包含失败信息即可。
	if rec.UserID != "u2" || rec.IPAddress != "9.9.9.9" {
		t.Fatalf("user_id=%q ip=%q, want u2/9.9.9.9", rec.UserID, rec.IPAddress)
	}
}

// TestLogService_CountLogins 验证合并后登录计数必须用 login_result 过滤：
// CountLogins(success) 只计成功、CountLogins(fail) 只计失败、CountLogins("") 计全部，
// 防止 CountByType("login") 把成功与失败登录一起算的隐患。
func TestLogService_CountLogins(t *testing.T) {
	fake := &fakeLogRepo{}
	s := NewLogService(fake, nil)
	now := time.Now()
	s.LogLogin(context.Background(), EventMeta{UserID: "u1"}, model.LoginTypePassword, model.LoginResultSuccess, "u1@example.com", "", "")
	s.LogLogin(context.Background(), EventMeta{UserID: "u2"}, model.LoginTypePassword, model.LoginResultFail, "u2@example.com", model.FailReasonPasswordWrong, "")
	s.Close()

	if n, _ := s.CountLogins(context.Background(), model.LoginResultSuccess, now, now.Add(time.Hour)); n != 1 {
		t.Fatalf("CountLogins(success) = %d, want 1", n)
	}
	if n, _ := s.CountLogins(context.Background(), model.LoginResultFail, now, now.Add(time.Hour)); n != 1 {
		t.Fatalf("CountLogins(fail) = %d, want 1", n)
	}
	if n, _ := s.CountLogins(context.Background(), "", now, now.Add(time.Hour)); n != 2 {
		t.Fatalf("CountLogins(\"\") = %d, want 2 (全部登录)", n)
	}
}

// TestLogService_LogLogin_WritesMetrics 验证成功写 login_count、失败写 login_fail_count，
// 失败率即可由 MonitorSumByType("login_count"/"login_fail_count") 直接求得，无需回查 audit_logs。
func TestLogService_LogLogin_WritesMetrics(t *testing.T) {
	fake := &fakeLogRepo{}
	s := NewLogService(fake, fake)
	s.LogLogin(context.Background(), EventMeta{UserID: "u1"}, model.LoginTypePassword, model.LoginResultSuccess, "u1@example.com", "", "")
	s.LogLogin(context.Background(), EventMeta{UserID: "u2"}, model.LoginTypePassword, model.LoginResultFail, "u2@example.com", model.FailReasonPasswordWrong, "")
	s.Close()

	if got := len(fake.metrics); got != 2 {
		t.Fatalf("metrics written = %d, want 2", got)
	}
	byType := map[string]int{}
	for _, m := range fake.metrics {
		byType[m.MetricType]++
	}
	if byType["login_count"] != 1 {
		t.Fatalf("login_count metrics = %d, want 1", byType["login_count"])
	}
	if byType["login_fail_count"] != 1 {
		t.Fatalf("login_fail_count metrics = %d, want 1", byType["login_fail_count"])
	}

	// 失败率即可直接由 MonitorSumByType 按计数器名求得，无需回查 audit_logs
	start, end := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	if v, _ := s.MonitorSumByType(context.Background(), "login_count", start, end); v != 1 {
		t.Fatalf("MonitorSumByType(login_count) = %v, want 1", v)
	}
	if v, _ := s.MonitorSumByType(context.Background(), "login_fail_count", start, end); v != 1 {
		t.Fatalf("MonitorSumByType(login_fail_count) = %v, want 1", v)
	}
}

// TestLogService_LogLogin_NilRepoNoop 验证 logRepo 为 nil 时 LogLogin 安全返回，不 panic。
func TestLogService_LogLogin_NilRepoNoop(t *testing.T) {
	s := &LogService{}
	s.LogLogin(context.Background(), EventMeta{UserID: "u1"},
		model.LoginTypeGoogle, model.LoginResultFail, "u1@example.com", "oauth_failed", "")
	time.Sleep(10 * time.Millisecond)
}

// TestLogService_QueryNilRepoNoop 验证三个查询方法在对应 repo 为 nil 时静默返回零值而非 panic，
// 与 writeLog/writeMetric 的 nil 守卫契约一致：任一 repo 为 nil 即 no-op。
func TestLogService_QueryNilRepoNoop(t *testing.T) {
	s := &LogService{} // logRepo 与 monitorRepo 均为 nil
	now := time.Now()
	start, end := now.Add(-time.Hour), now

	if n, err := s.CountByType(context.Background(), "login", start, end); n != 0 || err != nil {
		t.Fatalf("CountByType nil repo: got (%d,%v), want (0,nil)", n, err)
	}
	if n, err := s.CountLogins(context.Background(), model.LoginResultSuccess, start, end); n != 0 || err != nil {
		t.Fatalf("CountLogins nil repo: got (%d,%v), want (0,nil)", n, err)
	}
	if v, err := s.MonitorSumByType(context.Background(), "login_count", start, end); v != 0 || err != nil {
		t.Fatalf("MonitorSumByType nil repo: got (%v,%v), want (0,nil)", v, err)
	}
}
