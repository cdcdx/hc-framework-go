package auth

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// fakeMailSender 记录 Send 调用，用于断言注册确认邮件触发。
// 因 sendRegisterConfirmation 内部以独立 goroutine 异步发送，Send 的读写发生在不同 goroutine，
// 故用互斥锁保护，并用 done channel 让测试同步等待而非忙等（避免 data race）。
type fakeMailSender struct {
	mu     sync.Mutex
	called bool
	to     []string
	done   chan struct{}
}

func (f *fakeMailSender) Send(ctx context.Context, to []string, subject, bodyHTML string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.called = true
	f.to = to
	// 首次完成即关闭 done，通知等待方；重复调用不会 panic（channel 仅关闭一次）。
	select {
	case <-f.done:
	default:
		close(f.done)
	}
	return nil
}

// loadMailTestManager 构造带 mail.site_name 的最小配置管理器。
func loadMailTestManager(t *testing.T) *config.Manager {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	body := `
server:
  port: 8080
  mode: test
mail:
  enabled: true
  site_name: "TestSite"
`
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	m, err := config.LoadManager(p)
	if err != nil {
		t.Fatalf("load manager: %v", err)
	}
	return m
}

// TestSendRegisterConfirmation_NilMailer 验证 mailer 为 nil 时不发送、不 panic。
func TestSendRegisterConfirmation_NilMailer(t *testing.T) {
	svc := &AuthService{cfg: loadMailTestManager(t)}
	// 不应 panic
	svc.sendRegisterConfirmation(context.Background(), "a@b.com", "alice")
}

// TestSendRegisterConfirmation_SendsAsync 验证有 mailer 时异步发出，且收件人正确。
func TestSendRegisterConfirmation_SendsAsync(t *testing.T) {
	ms := &fakeMailSender{done: make(chan struct{})}
	svc := &AuthService{cfg: loadMailTestManager(t), mailer: ms}
	svc.sendRegisterConfirmation(context.Background(), "user@example.com", "")

	// 同步等待异步发送完成（done 关闭），避免忙等轮询导致的 data race。
	select {
	case <-ms.done:
	case <-time.After(2 * time.Second):
		t.Fatal("mail not sent within timeout")
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if !ms.called {
		t.Fatal("mail sender not called")
	}
	if len(ms.to) != 1 || ms.to[0] != "user@example.com" {
		t.Fatalf("mail to = %v, want [user@example.com]", ms.to)
	}
}

// deadlineMailSender 记录 Send 收到的 ctx 是否带截止（deadline），用于断言发送不携带
// 无截止的 context.Background()（否则 SMTP 挂起时后台 goroutine 永久泄漏，见 §3.66）。
type deadlineMailSender struct {
	mu          sync.Mutex
	hasDeadline bool
	done        chan struct{}
}

func (d *deadlineMailSender) Send(ctx context.Context, to []string, subject, bodyHTML string) error {
	d.mu.Lock()
	_, d.hasDeadline = ctx.Deadline()
	d.mu.Unlock()
	select {
	case <-d.done:
	default:
		close(d.done)
	}
	return nil
}

// TestSendRegisterConfirmation_UsesTimeoutContext 守卫 §3.66：注册确认邮件必须以带截止的
// ctx 调用 Send（而非 context.Background()）。若实现回退为无截止 ctx，fakeSender 的
// hasDeadline 为 false，本测试确定性失败——确保 SMTP 挂起时 goroutine 不会永久泄漏。
func TestSendRegisterConfirmation_UsesTimeoutContext(t *testing.T) {
	ms := &deadlineMailSender{done: make(chan struct{})}
	svc := &AuthService{cfg: loadMailTestManager(t), mailer: ms}
	svc.sendRegisterConfirmation(context.Background(), "u@x.com", "u")

	select {
	case <-ms.done:
	case <-time.After(2 * time.Second):
		t.Fatal("mail not sent within timeout")
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if !ms.hasDeadline {
		t.Fatal("Send called with context lacking a deadline: SMTP hang would leak the goroutine (§3.66)")
	}
}

// panickingMailSender 在 Send 中 panic，用于验证后台 goroutine 的 recover 能吞掉 panic、
// 绝不因一封邮件击垮整个进程（见 §3.66）。
type panickingMailSender struct {
	invoked chan struct{}
}

func (p *panickingMailSender) Send(ctx context.Context, to []string, subject, bodyHTML string) error {
	select {
	case <-p.invoked:
	default:
		close(p.invoked)
	}
	panic("simulated mailer failure")
}

// TestSendRegisterConfirmation_PanicRecovered 守卫 §3.66：邮件 goroutine 内 panic 必须被
// 吞掉。本测试依赖「进程在 Send 被调用、panic 被 recover 后依然存活」这一事实；若缺少
// recover，未捕获 panic 会直接终止整个测试进程（测试以非预期退出告终）。
func TestSendRegisterConfirmation_PanicRecovered(t *testing.T) {
	ms := &panickingMailSender{invoked: make(chan struct{})}
	svc := &AuthService{cfg: loadMailTestManager(t), mailer: ms}
	svc.sendRegisterConfirmation(context.Background(), "u@x.com", "u")

	select {
	case <-ms.invoked:
	case <-time.After(2 * time.Second):
		t.Fatal("mailer Send not invoked (recover test could not run)")
	}

	// 等待 goroutine 执行并触发 recover；若进程未被击垮，说明 recover 生效。
	time.Sleep(100 * time.Millisecond)
	// 若执行到这里，进程仍存活，recover 已捕获 panic（断言通过）。
}
