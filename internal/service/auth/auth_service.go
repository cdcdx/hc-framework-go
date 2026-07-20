package auth

import (
	"github.com/cdcdx/hc-framework-go/internal/cache"
	"github.com/cdcdx/hc-framework-go/internal/service/common"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/cdcdx/hc-framework-go/internal/config"
	"github.com/cdcdx/hc-framework-go/internal/event"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/mq"
	"github.com/cdcdx/hc-framework-go/internal/repository"
	"github.com/cdcdx/hc-framework-go/pkg/bcrypt"
	"github.com/cdcdx/hc-framework-go/pkg/mail"
	"golang.org/x/sync/semaphore"
	"runtime"
)

// Google OAuth 端点（授权码换 token、获取用户信息）
const (
	googleTokenEndpoint    = "https://oauth2.googleapis.com/token"
	googleUserInfoEndpoint = "https://openidconnect.googleapis.com/v1/userinfo"
)

// registerMailSendTimeout 注册确认邮件发送最长耗时，超时即放弃（不重试）。
// 用于约束后台发送 goroutine 的存活：避免 SMTP 服务器挂起时 goroutine 永久阻塞泄漏
// （注册洪峰下累积内存/连接，见 §3.66）。
const registerMailSendTimeout = 10 * time.Second

// googleHTTPClient 与 Google 通信的 HTTP 客户端（带超时，避免消费端/请求线程被拖死）
var googleHTTPClient = &http.Client{Timeout: 10 * time.Second}

// TokenBlacklister Token 黑名单接口（登出/改密/Refresh 轮转时失效旧 token）。
// 由 cache.Manager 实现（Redis SET jwt:blacklist:{jti}）。
type TokenBlacklister interface {
	AddBlacklist(ctx context.Context, jti string, ttl time.Duration) error
	IsBlacklisted(ctx context.Context, jti string) (bool, error)
}

// AuthService 认证服务
type AuthService struct {
	cfg       *config.Manager // 配置管理器（安全参数支持热更新）
	userRepo  repository.UserRepository
	logSvc    *common.LogService
	blacklist TokenBlacklister // 可选：Token 黑名单（nil 时跳过失效逻辑）
	producer  mq.Producer      // 可选：事件生产者（nil 时不发布事件）
	lockStore cache.AccountLockStore // 账号锁定存储（Redis 跨 Pod 共享 / 进程内回退）
	mailer    mail.Sender      // 可选：邮件发送器（nil 时跳过注册确认邮件）

	// bcryptSem 是 bcrypt 哈希/校验的准入信号量。
	// bcrypt 是纯 CPU 计算且【不响应 context 取消】，一旦放行就会占满核心直到算完；
	// 这导致即便 request_timeout / write_timeout 触发，运算也无法中断，CPU 被几千个并行的
	// bcrypt 吃光 → 请求排队数十秒 → 撞穿 write_timeout(30s) → 服务端直接关连接 → 客户端 EOF
	// （见 ws.js 压测日志：http_req_duration p95=51s、login 大量 EOF）。
	// 因此必须在进入 bcrypt 之前用信号量做准入控制：容量 = 核心数，每核同时只跑 1 个 bcrypt，
	// 超限立即返回 ErrServerBusy（→ handler 返回 503 快速失败），让负载卸载而非无限排队拖垮整机。
	bcryptSem *semaphore.Weighted
}

// NewAuthService 创建认证服务
func NewAuthService(cfg *config.Manager, userRepo repository.UserRepository, logSvc *common.LogService, blacklist TokenBlacklister, producer mq.Producer, lockStore cache.AccountLockStore, mailer mail.Sender) *AuthService {
	return &AuthService{
		cfg:       cfg,
		userRepo:  userRepo,
		logSvc:    logSvc,
		blacklist: blacklist,
		producer:  producer,
		lockStore: lockStore,
		mailer:    mailer,
		bcryptSem: semaphore.NewWeighted(int64(runtime.NumCPU())),
	}
}

// ErrServerBusy 表示 bcrypt 计算资源（CPU 核心）已耗尽，新请求拒绝准入。
// 由 handler 映射为 HTTP 503 + CodeServiceUnavailable（应用层负载卸载，区别于超时/崩溃）。
var ErrServerBusy = errors.New("bcrypt compute slots exhausted")

// acquireBcrypt 非阻塞获取一个 bcrypt 计算槽位（容量=核心数）。
// 返回 ErrServerBusy 表示当前 CPU 已被 bcrypt 占满，调用方应立即失败（503）而不是排队等待——
// bcrypt 不响应 ctx 取消，排队只会把尾延迟拉爆并最终撞穿 write_timeout 表现为 EOF。
func (s *AuthService) acquireBcrypt(ctx context.Context) error {
	if s.bcryptSem == nil {
		return nil
	}
	if s.bcryptSem.TryAcquire(1) {
		return nil
	}
	return ErrServerBusy
}

// releaseBcrypt 释放 bcrypt 计算槽位（配合 acquireBcrypt 使用）。
func (s *AuthService) releaseBcrypt() {
	if s.bcryptSem != nil {
		s.bcryptSem.Release(1)
	}
}

// publishEvent 发布事件（best-effort，失败不影响主流程；框架已具备 DLQ 降级）。
func (s *AuthService) publishEvent(ctx context.Context, eventType, key string, payload interface{}) {
	if s.producer == nil {
		return
	}
	_ = s.producer.Send(ctx, &event.Message{
		Timestamp: time.Now(),
		EventType: eventType,
		Key:       key,
		Payload:   payload,
	})
}

// sendRegisterConfirmation 发送注册确认邮件（best-effort）。
// 设计原则：
//   - mailer 为 nil（mail.enabled=false 或未配置）时直接返回，绝不阻塞注册；
//   - 异步 goroutine 发送，邮件 SMTP 抖动不影响注册响应与后续 JWT 签发；
//   - 发送失败仅告警日志，不向上抛错（注册已成功，不应因邮件失败而回滚）。
func (s *AuthService) sendRegisterConfirmation(ctx context.Context, email, username string) {
	if s.mailer == nil {
		return
	}
	// 避免无意义的 template 渲染：username 为空时用邮箱前缀兜底（与 generateUsername 一致）
	if username == "" {
		username = strings.Split(email, "@")[0]
	}
	// 在启动 goroutine 前捕获站点名，避免 goroutine 内读 s.cfg.Get() 与配置热加载竞态/潜在 nil 解引用（见 §3.66）。
	siteName := s.cfg.Get().Mail.SiteName
	go func() {
		// 防御性 recover：邮件发送在后台 goroutine 中执行，任何 panic（如 mailer 实现缺陷）
		// 都必须被吞掉，绝不能因一封注册确认邮件而击垮整个进程（见 §3.66）。
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[auth] mail: sendRegisterConfirmation panicked (email=%s): %v", email, r)
			}
		}()
		subject := "注册确认 - " + siteName
		body, err := mail.BuildConfirmRegisterHTML(mail.ConfirmRegisterData{
			SiteName:     siteName,
			Username:     username,
			Email:        email,
			RegisterTime: time.Now().Format("2006-01-02 15:04:05"),
			Subject:      subject,
		})
		if err != nil {
			log.Printf("[auth] mail: render register confirmation failed: %v", err)
			return
		}
		// 邮件发送不携带原始请求 ctx（防止请求取消导致发送中断），但须带超时 ctx：
		// 避免 SMTP 服务器挂起时后台 goroutine 永久阻塞泄漏（注册洪峰下累积内存/连接，见 §3.66）。
		sendCtx, cancel := context.WithTimeout(context.Background(), registerMailSendTimeout)
		defer cancel()
		if err := s.mailer.Send(sendCtx, []string{email}, subject, body); err != nil {
			log.Printf("[auth] mail: send register confirmation to %s failed: %v", email, err)
		}
	}()
}

// isAccountLocked 判断账号是否因连续失败而被锁定（依据 security.account_lock）。
// 存储为跨 Pod 共享（Redis），因此任意节点的登录失败都会累积并触发锁定。
func (s *AuthService) isAccountLocked(ctx context.Context, email string) bool {
	al := s.cfg.Get().Security.AccountLock
	if al.MaxFailures <= 0 {
		return false
	}
	until, err := s.lockStore.GetLockUntil(ctx, email)
	if err != nil {
		return false
	}
	return !until.IsZero() && time.Now().Before(until)
}

// recordLoginFailure 记录一次登录失败：原子递增计数、按 security.login_fail_delays 递增等待，
// 达到 security.account_lock.max_failures 后锁定 lock_duration 时长（跨 Pod 共享，单次原子操作）。
// 注意：先完成存储写入再 sleep，避免阻塞其它账号的登录请求。
func (s *AuthService) recordLoginFailure(ctx context.Context, email string) {
	al := s.cfg.Get().Security.AccountLock
	delays := s.cfg.Get().Security.LoginFailDelays

	// 原子记录失败：计数递增 + 达阈值锁定在同一次 Redis 操作中完成（单 key Hash + Lua）。
	// 存储不可用时降级为首次失败，避免误锁全部用户（可用性优先）。
	countTTL := al.LockDuration + time.Hour
	newCount, _, err := s.lockStore.RecordFailure(ctx, email, al.MaxFailures, countTTL, al.LockDuration)
	if err != nil {
		newCount = 1
	}

	var delay time.Duration
	if len(delays) > 0 {
		idx := newCount - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(delays) {
			idx = len(delays) - 1
		}
		delay = time.Duration(delays[idx]) * time.Second
	}

	if delay > 0 {
		time.Sleep(delay)
	}
}

// clearLoginFailure 登录成功后清除失败计数与锁定。
func (s *AuthService) clearLoginFailure(ctx context.Context, email string) {
	s.lockStore.Clear(ctx, email)
}

// RegisterRequest 注册请求
// 注意: Password 字段接收的是客户端 SHA256 哈希后的密码（64位十六进制），而非明文
type RegisterRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	Nickname string `json:"nickname,omitempty"`
}

// Register 注册
func (s *AuthService) Register(ctx context.Context, req *RegisterRequest) (*model.User, error) {
	// 校验邮箱
	if strings.TrimSpace(req.Email) == "" {
		return nil, fmt.Errorf("email is required")
	}
	req.Email = strings.ToLower(strings.TrimSpace(req.Email))

	// 校验密码强度
	if err := s.validatePassword(req.Password); err != nil {
		return nil, err
	}

	// 检查邮箱唯一性
	existing, err := s.userRepo.FindByEmail(ctx, req.Email)
	if err != nil {
		return nil, fmt.Errorf("check email: %w", err)
	}
	if existing != nil {
		return nil, ErrEmailRegistered
	}

	// 哈希密码
	hash, err := bcrypt.Hash(req.Password, s.bcryptCost())
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	// 创建用户（最多重试 3 次，防止极端并发下 UUID 碰撞）
	user, err := s.createUserWithRetry(ctx, req.Email, req.Nickname, hash)
	if err != nil {
		return nil, err
	}

	// 发布用户注册事件（事件驱动：任务系统每日登录、审计日志）
	s.publishEvent(ctx, event.EventUserRegistered, user.UserID, event.UserRegisteredPayload{
		UserID:    user.UserID,
		Email:     user.Email,
		LoginType: "password",
	})

	// 写注册日志 + 监控指标
	s.logSvc.LogRegister(ctx, common.BuildMeta(ctx, user.UserID), user.Email)

	// 发送注册确认邮件（best-effort：异步发送，失败仅告警，不影响注册结果与 JWT 签发）
	s.sendRegisterConfirmation(ctx, user.Email, user.Username)

	return user, nil
}

// LoginRequest 登录请求
// 注意: Password 字段接收的是客户端 SHA256 哈希后的密码（64位十六进制），而非明文
type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// LoginResult 登录结果
type LoginResult struct {
	User         *model.User `json:"user"`
	AccessToken  string      `json:"access_token"`
	RefreshToken string      `json:"refresh_token"`
}

// Login 邮箱登录
func (s *AuthService) Login(ctx context.Context, req *LoginRequest) (*LoginResult, error) {
	req.Email = strings.ToLower(strings.TrimSpace(req.Email))

	// 查询用户
	user, err := s.userRepo.FindByEmail(ctx, req.Email)
	if err != nil {
		return nil, fmt.Errorf("find user: %w", err)
	}
	if user == nil {
		s.recordLoginAttempt(ctx, req.Email, "", model.LoginResultFail, "user_not_found")
		return nil, ErrInvalidCredentials
	}

	// 检查账号状态（管理员禁用）
	if user.Status != "active" {
		s.recordLoginAttempt(ctx, user.UserID, user.UserID, model.LoginResultFail, "account_disabled")
		return nil, ErrAccountLocked
	}

	// 检查是否因连续失败被锁定（security.account_lock）
	if s.isAccountLocked(ctx, req.Email) {
		s.recordLoginAttempt(ctx, user.UserID, user.UserID, model.LoginResultFail, "account_locked")
		return nil, ErrAccountLocked
	}

	// 验证密码
	if bcrypt.Compare(user.PasswordHash, req.Password) != nil {
		s.recordLoginFailure(ctx, req.Email)
		s.recordLoginAttempt(ctx, user.UserID, user.UserID, model.LoginResultFail, "password_wrong")
		return nil, ErrInvalidCredentials
	}

	// 登录成功：清除失败计数
	s.clearLoginFailure(ctx, req.Email)

	// 生成 Token（复用 IssueTokens，避免重复实现 JWT 签发逻辑）
	result, err := s.IssueTokens(user)
	if err != nil {
		return nil, err
	}

	// 发布用户登录事件
	s.publishEvent(ctx, event.EventUserLoggedIn, user.UserID, event.UserLoggedInPayload{
		UserID:    user.UserID,
		LoginType: "password",
	})

	s.logSvc.LogLogin(ctx, common.BuildMeta(ctx, user.UserID), user.Email, true)
	s.recordLoginAttempt(ctx, user.UserID, user.UserID, model.LoginResultSuccess, "")

	return result, nil
}

// IssueTokens 为已认证的 user 签发 JWT（access/refresh）。
// 与 Login 的区别：不校验密码、不查库、不记录登录审计——调用方需保证 user 已经过认证。
// 用于注册成功后直接签发 Token（避免再调一次 Login 重复做 bcrypt 校验 + FindByEmail，
// 后者是 k6-auth 压测中 register 比 login 慢近一倍的主因，见 P0-1 修复）。
func (s *AuthService) IssueTokens(user *model.User) (*LoginResult, error) {
	accessToken, refreshToken, err := jwtGenerate(user.UserID, user.Email)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}
	return &LoginResult{
		User:         user,
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	}, nil
}

// recordLoginAttempt 记录一次登录尝试（userID/email 相同则用一个，找不到用户时 userID 可传空）。
func (s *AuthService) recordLoginAttempt(ctx context.Context, auditID, userID, result, reason string) {
	if userID == "" {
		userID = auditID
	}
	s.logSvc.RecordLogin(ctx, common.BuildMeta(ctx, auditID), model.LoginTypePassword, result, reason, "")
}

// GoogleOAuthLogin Google OAuth 登录/注册
func (s *AuthService) GoogleOAuthLogin(ctx context.Context, googleID, email, name, avatarURL string) (*LoginResult, error) {
	// 优先按 Google ID 查找
	user, err := s.userRepo.FindByGoogleID(ctx, googleID)
	if err != nil {
		return nil, fmt.Errorf("find by google id: %w", err)
	}

	if user == nil && email != "" {
		// 按 email 查找并绑定 Google ID
		user, err = s.userRepo.FindByEmail(ctx, email)
		if err != nil {
			return nil, fmt.Errorf("find by email: %w", err)
		}
		if user != nil {
			user.GoogleID = &googleID
			if err := s.userRepo.Update(ctx, user); err != nil {
				return nil, fmt.Errorf("bind google id: %w", err)
			}
		}
	}

	if user == nil {
		// 创建新用户
		user = &model.User{
			UserID:        uuid.New().String(),
			Username:      name,
			Email:         email,
			GoogleID:      &googleID,
			AvatarURL:     avatarURL,
			Status:        "active",
			PointsBalance: 0,
		}
		if err := s.userRepo.Create(ctx, user); err != nil {
			return nil, fmt.Errorf("create user: %w", err)
		}
		// 新注册用户发送确认邮件（best-effort）
		s.sendRegisterConfirmation(ctx, user.Email, user.Username)
	}

	// 生成 Token
	accessToken, refreshToken, err := jwtGenerate(user.UserID, user.Email)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	// 发布用户登录事件（审计日志，login_type=google）
	s.publishEvent(ctx, event.EventUserLoggedIn, user.UserID, event.UserLoggedInPayload{
		UserID:    user.UserID,
		LoginType: "google",
	})

	// 结构化登录记录（需求 §6.9，login_type=google）
	s.logSvc.RecordLogin(ctx, common.BuildMeta(ctx, user.UserID), model.LoginTypeGoogle, model.LoginResultSuccess, "", "")

	return &LoginResult{
		User:         user,
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	}, nil
}

// GoogleOAuthCode 使用前端回传的授权码（authorization code）完成 Google 登录/注册。
// 流程：授权码 → 交换 access_token → 拉取 userinfo → 复用 GoogleOAuthLogin 落库并签发 JWT。
func (s *AuthService) GoogleOAuthCode(ctx context.Context, code string) (*LoginResult, error) {
	info, err := s.exchangeGoogleCode(ctx, code)
	if err != nil {
		return nil, err
	}
	if info.Sub == "" {
		s.logSvc.RecordLogin(ctx, common.BuildMeta(ctx, info.Email), model.LoginTypeGoogle, model.LoginResultFail, "oauth_failed", "")
		return nil, ErrOAuthFailed
	}
	return s.GoogleOAuthLogin(ctx, info.Sub, info.Email, info.Name, info.Picture)
}

// googleUserInfo Google OpenID userinfo 响应
type googleUserInfo struct {
	Sub           string `json:"sub"` // Google 用户唯一 ID
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	Name          string `json:"name"`
	Picture       string `json:"picture"`
}

// exchangeGoogleCode 用授权码换取 access_token，再用 access_token 拉取用户信息。
func (s *AuthService) exchangeGoogleCode(ctx context.Context, code string) (*googleUserInfo, error) {
	g := s.cfg.Get().OAuth.Google
	if g.ClientID == "" || g.ClientSecret == "" {
		return nil, fmt.Errorf("google oauth not configured")
	}

	// 1. 授权码换 token
	form := url.Values{
		"code":          {code},
		"client_id":     {g.ClientID},
		"client_secret": {g.ClientSecret},
		"redirect_uri":  {g.RedirectURL},
		"grant_type":    {"authorization_code"},
	}
	tokenReq, err := http.NewRequestWithContext(ctx, http.MethodPost, googleTokenEndpoint,
		strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("build token request: %w", err)
	}
	tokenReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	tokenResp, err := googleHTTPClient.Do(tokenReq)
	if err != nil {
		return nil, fmt.Errorf("exchange code: %w", err)
	}
	defer tokenResp.Body.Close()

	tokenBody, _ := io.ReadAll(io.LimitReader(tokenResp.Body, 1<<20))
	if tokenResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: token endpoint status=%d body=%s", ErrOAuthFailed, tokenResp.StatusCode, string(tokenBody))
	}
	var token struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(tokenBody, &token); err != nil {
		return nil, fmt.Errorf("decode token response: %w", err)
	}
	if token.AccessToken == "" {
		return nil, fmt.Errorf("%w: empty access_token", ErrOAuthFailed)
	}

	// 2. 用 access_token 拉取用户信息
	infoReq, err := http.NewRequestWithContext(ctx, http.MethodGet, googleUserInfoEndpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build userinfo request: %w", err)
	}
	infoReq.Header.Set("Authorization", "Bearer "+token.AccessToken)

	infoResp, err := googleHTTPClient.Do(infoReq)
	if err != nil {
		return nil, fmt.Errorf("fetch userinfo: %w", err)
	}
	defer infoResp.Body.Close()

	infoBody, _ := io.ReadAll(io.LimitReader(infoResp.Body, 1<<20))
	if infoResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: userinfo status=%d body=%s", ErrOAuthFailed, infoResp.StatusCode, string(infoBody))
	}
	var info googleUserInfo
	if err := json.Unmarshal(infoBody, &info); err != nil {
		return nil, fmt.Errorf("decode userinfo: %w", err)
	}
	return &info, nil
}

// RefreshToken 刷新 Token
func (s *AuthService) RefreshToken(ctx context.Context, refreshTokenStr string) (*LoginResult, error) {
	// 验证 Refresh Token
	claims, err := jwtValidate(refreshTokenStr)
	if err != nil {
		return nil, ErrTokenInvalid
	}

	user, err := s.userRepo.FindByID(ctx, claims.UserID)
	if err != nil {
		return nil, fmt.Errorf("find user: %w", err)
	}
	if user == nil {
		return nil, ErrTokenInvalid
	}

	// Refresh Token 轮转：旧 Refresh Token 立即失效（加入黑名单，TTL = 剩余有效期），
	// 防止旧 refresh 在 7d 内被重放多次刷新（需求 §8）。
	if s.blacklist != nil && claims.ID != "" {
		if ttl := time.Until(claims.ExpiresAt.Time); ttl > 0 {
			_ = s.blacklist.AddBlacklist(ctx, claims.ID, ttl)
		}
	}

	// 生成新 Token 对
	accessToken, refreshToken, err := jwtGenerate(user.UserID, user.Email)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	return &LoginResult{
		User:         user,
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	}, nil
}

// ChangePasswordRequest 修改密码请求
type ChangePasswordRequest struct {
	OldPassword string `json:"old_password"`
	NewPassword string `json:"new_password"`
}

// ChangePassword 修改密码：
//  1. 校验旧密码（旧密码错误或用户不存在统一返回 ErrInvalidCredentials，避免账号枚举）；
//  2. 校验新密码强度；
//  3. 更新密码哈希与 password_changed_at；
//  4. 将 pwd_change:{user_id} 写入 Token 黑名单（TTL=RefreshTTL），
//     使所有已签发 Token 立即失效（auth 中间件对每个请求校验该黑名单）。
func (s *AuthService) ChangePassword(ctx context.Context, userID, oldPassword, newPassword string) error {
	user, err := s.userRepo.FindByID(ctx, userID)
	if err != nil {
		return fmt.Errorf("find user: %w", err)
	}
	if user == nil {
		return ErrInvalidCredentials
	}

	// 校验旧密码（bcrypt 准入）
	if err := s.acquireBcrypt(ctx); err != nil {
		return err
	}
	defer s.releaseBcrypt()

	if bcrypt.Compare(user.PasswordHash, oldPassword) != nil {
		return ErrInvalidCredentials
	}

	// 校验新密码强度
	if err := s.validatePassword(newPassword); err != nil {
		return fmt.Errorf("validate new password: %w", err)
	}

	hash, err := bcrypt.Hash(newPassword, s.bcryptCost())
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}

	if err := s.userRepo.UpdatePassword(ctx, userID, hash, time.Now()); err != nil {
		return fmt.Errorf("update password: %w", err)
	}

	// 密码修改后强制所有旧 Token 失效（需求 §8）
	if s.blacklist != nil {
		_ = s.blacklist.AddBlacklist(ctx, "pwd_change:"+userID, s.cfg.Get().Auth.JWT.RefreshTTL)
	}

	return nil
}

func (s *AuthService) generateUsername(email, nickname string) string {
	if nickname != "" {
		return nickname
	}
	parts := strings.Split(email, "@")
	if len(parts) > 0 {
		return parts[0]
	}
	return email
}

// maxUUIDRetries UUID 碰撞最大重试次数
const maxUUIDRetries = 3

// createUserWithRetry 先查重再入库，最多尝试 maxUUIDRetries 次防止 UUID 碰撞
func (s *AuthService) createUserWithRetry(ctx context.Context, email, nickname, passwordHash string) (*model.User, error) {
	for i := 0; i < maxUUIDRetries; i++ {
		userID := uuid.New().String()

		// 入库前先查 UserID 是否已存在（防御 UUID 碰撞）
		existing, err := s.userRepo.FindByID(ctx, userID)
		if err != nil {
			return nil, fmt.Errorf("check user id: %w", err)
		}
		if existing != nil {
			// UserID 碰撞，重新生成
			continue
		}

		user := &model.User{
			UserID:        userID,
			Username:      s.generateUsername(email, nickname),
			Email:         email,
			PasswordHash:  passwordHash,
			Status:        "active",
			PointsBalance: 0,
		}

		err = s.userRepo.Create(ctx, user)
		if err == nil {
			return user, nil
		}

		// 邮箱唯一索引冲突 → 直接返回，不重试
		if isUniqueViolation(err) {
			return nil, ErrEmailRegistered
		}

		// 并发竞争下 Create 时 UserID 可能已被其他请求写入 → 重试
	}

	return nil, fmt.Errorf("create user failed after %d attempts", maxUUIDRetries)
}

// isUniqueViolation 判断 DB 错误是否由唯一约束冲突引起
func isUniqueViolation(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE") || strings.Contains(msg, "unique")
}

// bcryptCost 返回配置中的 bcrypt cost（auth.password.bcrypt_cost），
// 未配置(<=0)时回退到默认 12，确保该配置项真实生效。
func (s *AuthService) bcryptCost() int {
	if c := s.cfg.Get().Auth.Password.BcryptCost; c > 0 {
		return c
	}
	return bcrypt.DefaultCost
}

// validatePassword 校验密码格式与强度。
// 设计约定：前端优先对明文做 SHA256 后在请求体中传输（64 位十六进制），
// 服务端据此存储 bcrypt(SHA256(pw))，避免明文在网络上直接出现。
// 为兼容未预哈希的客户端，非 64 位字符串按明文处理并执行强度策略；
// 64 位字符串则要求为合法 hex（即 SHA256 哈希）。两种形式内部一致，
// 注册与登录须使用同一形式。
func (s *AuthService) validatePassword(password string) error {
	if len(password) != 64 {
		// return fmt.Errorf("password must be a 64-character hex-encoded SHA256 hash")

		cfg := s.cfg.Get().Auth.Password
		if len(password) < cfg.MinLength {
			return fmt.Errorf("password must be at least %d characters", cfg.MinLength)
		}

		categories := 0
		if cfg.RequireUpper && containsUpper(password) {
			categories++
		}
		if cfg.RequireLower && containsLower(password) {
			categories++
		}
		if cfg.RequireDigit && containsDigit(password) {
			categories++
		}
		if cfg.RequireSpecial && containsSpecial(password) {
			categories++
		}

		if categories < cfg.RequireCategories {
			return fmt.Errorf("password must contain at least %d of: uppercase, lowercase, digit, special character", cfg.RequireCategories)
		}
		return nil
	}
	if !isHexString(password) {
		return fmt.Errorf("password must be a valid hex-encoded SHA256 hash")
	}
	return nil
}

// isHexString 检查字符串是否全为十六进制字符
func isHexString(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func containsUpper(s string) bool {
	for _, c := range s {
		if c >= 'A' && c <= 'Z' {
			return true
		}
	}
	return false
}
func containsLower(s string) bool {
	for _, c := range s {
		if c >= 'a' && c <= 'z' {
			return true
		}
	}
	return false
}
func containsDigit(s string) bool {
	for _, c := range s {
		if c >= '0' && c <= '9' {
			return true
		}
	}
	return false
}
func containsSpecial(s string) bool {
	for _, c := range s {
		if !(c >= 'A' && c <= 'Z') && !(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') {
			return true
		}
	}
	return false
}

// 服务层自定义错误
var (
	ErrEmailRegistered    = fmt.Errorf("email already registered")
	ErrInvalidCredentials = fmt.Errorf("invalid email or password")
	ErrAccountLocked      = fmt.Errorf("account locked")
	ErrTokenInvalid       = fmt.Errorf("invalid token")
	ErrOAuthFailed        = fmt.Errorf("google oauth failed")
)

// 编译期断言：cache.Manager 实现 TokenBlacklister 接口
var _ TokenBlacklister = (*cache.Manager)(nil)
