package config

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"

	"github.com/cdcdx/hc-framework-go/pkg/logger"
)

// Manager 支持运行时热更新的配置管理器（需求 §9）。
//
// 内部以 atomic.Pointer 持有不可变的 *Config：Reload 时重新解析 YAML 并原子替换指针，
// 各组件在请求路径上通过 Get() 获取当前 *Config，从而无锁、无数据竞争地感知最新配置
// （避免了「并发读写同一结构体字段」的 data race）。
//
// 触发方式：
//   - 文件监听：LoadManager 注册 Viper OnConfigChange（编辑器保存即重载，带去抖）；
//   - 信号：Watch 监听 SIGHUP；
//   - HTTP API：HealthHandler.Reload（POST /debug/reload）。
type Manager struct {
	path string
	v    *viper.Viper
	ptr  atomic.Pointer[Config]
	mu   sync.Mutex // 保护 Reload 重入（文件监听与 SIGHUP 可能并发触发）+ watchTimer
	ver  atomic.Int64

	watchTimer *time.Timer // 文件监听去抖定时器（nil 表示未安排）
	closed     atomic.Bool // Close 后阻止任何待执行的去抖重载
}

// watchDebounce 文件监听去抖窗口：编辑器保存常触发多次 fsnotify 事件，且可能短暂写入
// 中间态/非法内容（如用户正在编辑的 port: 0）。合并窗口内的连续事件为一次重载，待文件
// 落定后再读取最终内容，可消除瞬时非法配置引发的「reload failed, keep old」误报警告，
// 同时避免一次保存触发多次昂贵重载。SIGHUP / HTTP 触发的重载不受影响，仍立即执行。
const watchDebounce = 300 * time.Millisecond

// LoadManager 加载配置并返回可热更新的管理器。
func LoadManager(path string) (*Manager, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("yaml")
	v.SetEnvPrefix("APP")
	// 允许 APP_DATABASE_BUSINESS_DRIVER 这类「下划线」环境变量覆盖
	// database.business.driver 这类「点号」配置键（否则 AutomaticEnv 只会去
	// 查找带点的 APP_DATABASE.BUSINESS.DRIVER，导致 make run-mysql 等
	// 通过环境变量切换驱动/DSN 的覆盖被静默忽略）。
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config failed: %w", err)
	}
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config failed: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config invalid: %w", err)
	}

	m := &Manager{path: path, v: v}
	m.ptr.Store(&cfg)

	// 文件监听：配置文件变更（保存）时自动重载（经去抖合并连续事件）
	v.OnConfigChange(func(e fsnotify.Event) {
		m.scheduleWatchReload(e.Op)
	})
	v.WatchConfig()
	return m, nil
}

// scheduleWatchReload 将文件监听触发的重载并入去抖窗口：窗口内任意次数事件只会在
// 最后一次事件后延迟 watchDebounce 执行一次 Reload，从而读取落定后的最终文件内容，
// 避免编辑器多事件/中间态导致的瞬时非法重载告警。
func (m *Manager) scheduleWatchReload(op fsnotify.Op) {
	if m.closed.Load() {
		return
	}
	m.mu.Lock()
	if m.watchTimer != nil {
		m.watchTimer.Stop()
	}
	m.watchTimer = time.AfterFunc(watchDebounce, func() {
		if m.closed.Load() {
			return
		}
		if err := m.Reload(); err != nil {
			logger.L().Sugar().Warnf("config file watch reload failed: %v", err)
		} else {
			logger.L().Sugar().Infof("config reloaded via file watch (%s)", op)
		}
	})
	m.mu.Unlock()
}

// Close 停止文件监听去抖定时器，阻止任何待执行的重载回调（例如测试结束后释放资源）。
// 已生效的配置快照不受影响；SIGHUP / HTTP 触发路径不依赖本方法。
func (m *Manager) Close() {
	m.closed.Store(true)
	m.mu.Lock()
	if m.watchTimer != nil {
		m.watchTimer.Stop()
		m.watchTimer = nil
	}
	m.mu.Unlock()
}

// NewManager 基于内存中的配置构造管理器（不读取文件、不启动文件监听）。
// 适用于以编程方式注入配置：测试、内嵌默认配置，或已由其它途径解析好的 *Config。
// 注意：返回的实例不做热更新（无文件监听/信号监听），Get() 始终返回本次注入的快照。
func NewManager(cfg *Config) *Manager {
	m := &Manager{}
	m.ptr.Store(cfg)
	return m
}

// Get 返回当前生效的（不可变）配置快照。可在任意 goroutine 并发调用。
func (m *Manager) Get() *Config {
	return m.ptr.Load()
}

// Reload 重新读取并解析配置文件，原子替换生效配置。
//
// 注意：本方法不使用被 viper WatchConfig 监听的 m.v，而是每次用全新的 viper 实例
// 从磁盘重新解析。原因：viper v1.19 的 WatchConfig goroutine 在文件变更时会先自行
// 调用 v.ReadInConfig() 刷新 m.v，再回调 OnConfigChange；若 Reload 也读写同一个 m.v，
// 会与监听 goroutine 的并发读产生数据竞争（DATA RACE）。改用独立 viper 实例后，
// Reload 与文件监听各自操作不同的 viper 对象，竞争消除；同时 SIGHUP / HTTP 重载
// 重新读盘也更符合预期。
func (m *Manager) Reload() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cfg, err := loadFromFile(m.path)
	if err != nil {
		return fmt.Errorf("reload read config failed: %w", err)
	}
	// 校验失败：保留旧配置不替换，避免热更新把服务搞崩
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("reload config invalid, keep old: %w", err)
	}
	m.ptr.Store(cfg)
	m.ver.Add(1)
	return nil
}

// Version 返回配置版本（每次成功 Reload +1），便于观测热更新是否生效。
func (m *Manager) Version() int64 {
	return m.ver.Load()
}

// Watch 启动后台监听：收到 SIGHUP 时重新加载配置（需求 §9：Signal 触发重载）。
// 文件监听由 LoadManager 注册，此处仅补充信号通道。ctx 取消时停止监听。
func (m *Manager) Watch(ctx context.Context) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-ctx.Done():
				signal.Stop(ch)
				return
			case <-ch:
				if err := m.Reload(); err != nil {
					logger.L().Sugar().Warnf("config SIGHUP reload failed: %v", err)
				} else {
					logger.L().Sugar().Infof("config reloaded via SIGHUP (version=%d)", m.Version())
				}
			}
		}
	}()
}
