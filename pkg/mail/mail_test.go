package mail

import (
	"context"
	"testing"
	"time"
)

// 编译期断言：fakeSender 满足 Sender 接口，接口变更时本测试会率先失败，守住契约。
var _ Sender = (*fakeSender)(nil)

// fakeSender 记录调用参数，便于断言。
type fakeSender struct {
	called bool
	to     []string
	subj   string
	body   string
}

func (f *fakeSender) Send(ctx context.Context, to []string, subject, bodyHTML string) error {
	f.called = true
	f.to = to
	f.subj = subject
	f.body = bodyHTML
	return nil
}

func TestSenderInterfaceContract(t *testing.T) {
	// 通过 Sender 接口调用 fake，验证参数透传与返回语义正确，守住接口契约。
	var s Sender = &fakeSender{}
	if err := s.Send(context.Background(), []string{"a@b.com"}, "标题", "<p>正文</p>"); err != nil {
		t.Fatalf("Send returned error: %v", err)
	}
	f := s.(*fakeSender)
	if !f.called {
		t.Fatal("Send should have been invoked")
	}
	if len(f.to) != 1 || f.to[0] != "a@b.com" {
		t.Fatalf("to = %v, want [a@b.com]", f.to)
	}
	if f.subj != "标题" || f.body != "<p>正文</p>" {
		t.Fatalf("subject/body not recorded: subj=%q body=%q", f.subj, f.body)
	}
}

func TestNewSMTPSender_Disabled(t *testing.T) {
	if s := NewSMTPSender(SMTPConfig{Enabled: false}); s != nil {
		t.Fatal("disabled sender should be nil")
	}
	if s := NewSMTPSender(SMTPConfig{Enabled: true, Host: ""}); s != nil {
		t.Fatal("sender without host should be nil")
	}
}

func TestNewSMTPSender_Enabled(t *testing.T) {
	s := NewSMTPSender(SMTPConfig{Enabled: true, Host: "smtp.example.com", Port: 465})
	if s == nil {
		t.Fatal("enabled sender with host should not be nil")
	}
	if s.addr() != "smtp.example.com:465" {
		t.Fatalf("addr = %s, want smtp.example.com:465", s.addr())
	}
}

func TestBuildConfirmRegisterHTML(t *testing.T) {
	html, err := BuildConfirmRegisterHTML(ConfirmRegisterData{
		SiteName: "HC", Username: "alice", Email: "a@b.com", RegisterTime: "2026-07-10 12:00:00",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{"alice", "a@b.com", "HC"} {
		if !contains(html, want) {
			t.Fatalf("rendered html missing %q", want)
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}

// TestDeadlineFromCtx 守卫 §3.67：deadlineFromCtx 须正确从 ctx 推导截止；
// ctx 有截止则原样返回，无截止则回退默认超时（未来时间点）。
func TestDeadlineFromCtx(t *testing.T) {
	s := &SMTPSender{}
	want := time.Now().Add(1234 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), want)
	defer cancel()
	if got := s.deadlineFromCtx(ctx); !got.Equal(want) {
		t.Fatalf("deadlineFromCtx = %v, want %v", got, want)
	}
	got2 := s.deadlineFromCtx(context.Background())
	if got2.Before(time.Now()) {
		t.Fatal("expected future deadline when ctx has none")
	}
	d := time.Until(got2)
	if d < defaultSMTPSendTimeout-2*time.Second || d > defaultSMTPSendTimeout+2*time.Second {
		t.Fatalf("default deadline off: %v", d)
	}
}

// TestSMTPSender_SendHonorsContextDeadline 守卫 §3.67：Send 必须遵循 ctx 截止，
// 对不可达/挂起的 SMTP 主机在 ctx 截止（300ms）内返回，而非 OS 默认连接超时（分钟级）。
// 若旧实现（tls.Dial/smtp.Dial 完全忽略 ctx）被还原，本测试将因阻塞超过 3s 而失败——
// 证明 ctx 真正约束了 SMTP I/O，注册邮件 goroutine 不会永久泄漏。
func TestSMTPSender_SendHonorsContextDeadline(t *testing.T) {
	cases := []struct {
		name   string
		useTLS bool
		port   int
	}{
		{"starttls_587", false, 587},
		{"implicit_tls_465", true, 465},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// 10.255.255.1 为非路由保留地址：SYN 被丢弃，正常会触发 OS 级连接超时（分钟级）。
			s := NewSMTPSender(SMTPConfig{Enabled: true, Host: "10.255.255.1", Port: c.port, UseTLS: c.useTLS})
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			start := time.Now()
			err := s.Send(ctx, []string{"a@b.com"}, "subj", "<p>body</p>")
			elapsed := time.Since(start)
			if err == nil {
				t.Fatal("expected error for unreachable SMTP host")
			}
			if elapsed > 3*time.Second {
				t.Fatalf("Send did not honor ctx deadline: took %v (goroutine would leak on real SMTP hang, §3.67)", elapsed)
			}
		})
	}
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
