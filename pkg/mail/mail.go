// Package mail 提供邮件发送能力，抽象出 Sender 接口以便注入与替换（SMTP / 第三方 API）。
// 当前内置标准库 net/smtp 实现的 SMTPSender，无第三方依赖；发送为同步阻塞调用，
// 调用方（如注册流程）应自行决定是同步发还是 go 异步发、失败是否影响主流程。
package mail

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/smtp"
	"strings"
	"time"
)

// Sender 邮件发送接口，便于测试 mock 与生产替换。
type Sender interface {
	// Send 发送一封邮件。to 为收件人地址（可多个）；subject 主题；bodyHTML 为 HTML 正文，
	// 同时会作为纯文本正文的兜底（简单场景无需两套模板）。
	Send(ctx context.Context, to []string, subject, bodyHTML string) error
}

// SMTPConfig SMTP 客户端配置。
type SMTPConfig struct {
	// Enabled 总开关；false 时 NewSMTPSender 返回 nil，调用方应据此跳过发送。
	Enabled bool `mapstructure:"enabled"`
	// Host SMTP 服务器地址（不含端口），如 smtp.example.com。
	Host string `mapstructure:"host"`
	// Port 端口（465 通常隐式 TLS，587 通常 STARTTLS，25 明文）。
	Port int `mapstructure:"port"`
	// Username 认证用户名（为空表示不认证）。
	Username string `mapstructure:"username"`
	// Password 认证密码 / 授权码。
	Password string `mapstructure:"password"`
	// From 发件人邮箱地址。
	From string `mapstructure:"from"`
	// FromName 发件人显示名。
	FromName string `mapstructure:"from_name"`
	// UseTLS 是否使用隐式 TLS（端口 465 场景）。
	UseTLS bool `mapstructure:"use_tls"`
	// InsecureSkipVerify 跳过 TLS 证书校验（仅测试/内网自签场景开启）。
	InsecureSkipVerify bool `mapstructure:"insecure_skip_verify"`
}

// SMTPSender 基于标准库 net/smtp 的实现。
type SMTPSender struct {
	cfg SMTPConfig
}

// NewSMTPSender 根据配置创建 SMTP 发送器；Enabled=false 或 Host 为空时返回 nil。
func NewSMTPSender(cfg SMTPConfig) *SMTPSender {
	if !cfg.Enabled || cfg.Host == "" {
		return nil
	}
	return &SMTPSender{cfg: cfg}
}

// addr 返回 SMTP 服务器地址 host:port。
func (s *SMTPSender) addr() string {
	return fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
}

// auth 构造 SMTP 认证（无用户名时返回 nil，即匿名发送）。
func (s *SMTPSender) auth() smtp.Auth {
	if s.cfg.Username == "" {
		return nil
	}
	// 匿名身份信息（@ 符号前）多数服务器可填空或用户名；用 username 即可。
	return smtp.PlainAuth("", s.cfg.Username, s.cfg.Password, s.cfg.Host)
}

// buildMessage 拼装 RFC 5322 邮件原文。
func (s *SMTPSender) buildMessage(to []string, subject, bodyHTML string) []byte {
	fromDisplay := s.cfg.From
	if s.cfg.FromName != "" {
		fromDisplay = fmt.Sprintf("%s <%s>", s.cfg.FromName, s.cfg.From)
	}
	var b strings.Builder
	b.WriteString("From: " + fromDisplay + "\r\n")
	b.WriteString("To: " + strings.Join(to, ", ") + "\r\n")
	b.WriteString("Subject: " + subject + "\r\n")
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: text/html; charset=\"UTF-8\"\r\n")
	b.WriteString("\r\n")
	b.WriteString(bodyHTML)
	return []byte(b.String())
}

// defaultSMTPSendTimeout Send 在 ctx 无截止时的兜底超时，避免 SMTP 服务器挂起时
// 调用方 goroutine 永久阻塞（见 §3.67 / §3.66）。
const defaultSMTPSendTimeout = 10 * time.Second

// Send 发送邮件：按 UseTLS 选择隐式 TLS 或 STARTTLS，失败返回 error。
// Send 遵循传入 ctx 的截止时间：以 Dialer.Deadline + conn.SetDeadline 约束建连与
// 整个 SMTP 交互（MAIL/RCPT/DATA）。ctx 无截止时回退 defaultSMTPSendTimeout。
// 这令调用方（如注册确认邮件的 10s 超时 ctx，见 §3.66）能真正约束本次发送，
// 避免 SMTP 服务器挂起导致 goroutine 永久泄漏——旧实现完全忽略 ctx，超时形同虚设（见 §3.67）。
func (s *SMTPSender) Send(ctx context.Context, to []string, subject, bodyHTML string) error {
	if len(to) == 0 {
		return fmt.Errorf("mail: no recipient")
	}

	msg := s.buildMessage(to, subject, bodyHTML)
	addr := s.addr()

	deadline := s.deadlineFromCtx(ctx)

	var sendErr error
	if s.cfg.UseTLS {
		// 隐式 TLS（SMTPS，端口 465）：先建立 TLS 连接再 SMTP 握手。
		tlsCfg := &tls.Config{
			ServerName:         s.cfg.Host,
			InsecureSkipVerify: s.cfg.InsecureSkipVerify,
		}
		dialer := &net.Dialer{Deadline: deadline}
		conn, err := tls.DialWithDialer(dialer, "tcp", addr, tlsCfg)
		if err != nil {
			return fmt.Errorf("mail: tls dial %s: %w", addr, err)
		}
		// 约束整个 SMTP 会话（Auth/Mail/Rcpt/Data）的读写截止，避免挂起。
		_ = conn.SetDeadline(deadline)
		defer conn.Close()
		client, err := smtp.NewClient(conn, s.cfg.Host)
		if err != nil {
			return fmt.Errorf("mail: new client: %w", err)
		}
		defer client.Quit()
		sendErr = s.deliver(client, to, msg)
	} else {
		// 明文 / STARTTLS（端口 25/587）：先 plain 连接，再尝试升级 STARTTLS。
		dialer := &net.Dialer{Deadline: deadline}
		conn, err := dialer.Dial("tcp", addr)
		if err != nil {
			return fmt.Errorf("mail: dial %s: %w", addr, err)
		}
		// 约束 STARTTLS 升级前的交互截止（升级后 net/smtp 内部替换底层 conn，
		// 此处无法对其设截止；但建连已由 Dialer.Deadline 约束，且升级后 DATA 通常很快，见 §3.67）。
		_ = conn.SetDeadline(deadline)
		defer conn.Close()
		client, err := smtp.NewClient(conn, s.cfg.Host)
		if err != nil {
			return fmt.Errorf("mail: new client: %w", err)
		}
		defer client.Quit()
		if ok, _ := client.Extension("STARTTLS"); ok {
			tlsCfg := &tls.Config{
				ServerName:         s.cfg.Host,
				InsecureSkipVerify: s.cfg.InsecureSkipVerify,
			}
			if err := client.StartTLS(tlsCfg); err != nil {
				return fmt.Errorf("mail: starttls: %w", err)
			}
		}
		sendErr = s.deliver(client, to, msg)
	}
	return sendErr
}

// deadlineFromCtx 由 ctx 截止时间推导 SMTP I/O 截止；ctx 无截止则用默认超时。
func (s *SMTPSender) deadlineFromCtx(ctx context.Context) time.Time {
	if dl, ok := ctx.Deadline(); ok {
		return dl
	}
	return time.Now().Add(defaultSMTPSendTimeout)
}

// deliver 执行 SMTP 信封交互（MAIL/RCPT/DATA）。
func (s *SMTPSender) deliver(client *smtp.Client, to []string, msg []byte) error {
	if auth := s.auth(); auth != nil {
		if err := client.Auth(auth); err != nil {
			return fmt.Errorf("mail: auth: %w", err)
		}
	}
	if err := client.Mail(s.cfg.From); err != nil {
		return fmt.Errorf("mail: mail from: %w", err)
	}
	for _, rcpt := range to {
		if err := client.Rcpt(rcpt); err != nil {
			return fmt.Errorf("mail: rcpt %s: %w", rcpt, err)
		}
	}
	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("mail: data: %w", err)
	}
	if _, err := w.Write(msg); err != nil {
		return fmt.Errorf("mail: write body: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("mail: close data: %w", err)
	}
	return nil
}
