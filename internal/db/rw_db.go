package db

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
)

// contextKey 上下文键类型（避免冲突）
type contextKey string

const forceMasterKey contextKey = "rw_force_master"

// UseMaster 强制后续操作使用主库（用于写后读一致性场景）
func UseMaster(ctx context.Context) context.Context {
	return context.WithValue(ctx, forceMasterKey, true)
}

// isMasterForced 检查上下文是否要求强制主库
func isMasterForced(ctx context.Context) bool {
	v, _ := ctx.Value(forceMasterKey).(bool)
	return v
}

// RWDB 读写分离数据库包装器
// 读操作走从库（负载均衡），写操作/事务走主库
type RWDB struct {
	master       *gorm.DB
	slaves       []*gorm.DB
	mu           sync.RWMutex
	slaveIdx     uint64            // round-robin 索引
	unhealthySet map[*gorm.DB]bool // 不健康的从库集合（用于故障摘除）
}

// NewRWDB 创建读写分离数据库
// slaves 为空时，所有操作都走 master
func NewRWDB(master *gorm.DB, slaves []*gorm.DB) *RWDB {
	return &RWDB{
		master:       master,
		slaves:       slaves,
		unhealthySet: make(map[*gorm.DB]bool),
	}
}

// Read 返回读连接（走从库，若上下文强制主库或无从库则走主库）
func (rw *RWDB) Read(ctx context.Context) *gorm.DB {
	if isMasterForced(ctx) || len(rw.slaves) == 0 {
		return rw.master.WithContext(ctx)
	}
	s := rw.pickSlave()
	if s == nil {
		return rw.master.WithContext(ctx)
	}
	return s.WithContext(ctx)
}

// Write 返回写连接（始终走主库）
func (rw *RWDB) Write(ctx context.Context) *gorm.DB {
	return rw.master.WithContext(ctx)
}

// Master 返回主库连接（用于需要显式使用主库的场景，如 DDL）
func (rw *RWDB) Master() *gorm.DB {
	return rw.master
}

// Transaction 在主库上执行事务
func (rw *RWDB) Transaction(ctx context.Context, fn func(tx *gorm.DB) error) error {
	return rw.master.WithContext(ctx).Transaction(fn)
}

// Ping 健康检查：主库必须可达，从库尽力而为
func (rw *RWDB) Ping(ctx context.Context) error {
	sqlDB, err := rw.master.DB()
	if err != nil {
		return fmt.Errorf("master db: %w", err)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("master ping: %w", err)
	}

	rw.mu.RLock()
	slaves := make([]*gorm.DB, len(rw.slaves))
	copy(slaves, rw.slaves)
	rw.mu.RUnlock()

	for i, s := range slaves {
		sqlDB, err := s.DB()
		if err != nil {
			return fmt.Errorf("slave[%d] db: %w", i, err)
		}
		if err := sqlDB.PingContext(ctx); err != nil {
			return fmt.Errorf("slave[%d] ping: %w", i, err)
		}
	}
	return nil
}

// Close 关闭所有连接
func (rw *RWDB) Close() error {
	var errs []error

	closeDB := func(label string, db *gorm.DB) {
		if db == nil {
			return
		}
		sqlDB, err := db.DB()
		if err != nil {
			errs = append(errs, fmt.Errorf("%s get sql.DB: %w", label, err))
			return
		}
		if err := sqlDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("%s close: %w", label, err))
		}
	}

	closeDB("master", rw.master)
	for i, s := range rw.slaves {
		closeDB(fmt.Sprintf("slave[%d]", i), s)
	}

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}

// pickSlave round-robin 选取从库（跳过不健康的从库）
func (rw *RWDB) pickSlave() *gorm.DB {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	if len(rw.slaves) == 0 {
		return nil
	}

	// 先收集健康从库
	healthy := make([]*gorm.DB, 0, len(rw.slaves))
	for _, s := range rw.slaves {
		if !rw.unhealthySet[s] {
			healthy = append(healthy, s)
		}
	}
	if len(healthy) == 0 {
		return nil // 所有从库都不健康，上层降级到主库
	}

	idx := atomic.AddUint64(&rw.slaveIdx, 1) % uint64(len(healthy))
	return healthy[idx]
}

// ============================================
// RWDB 创建辅助方法
// ============================================

// CreateRWDBFromMySQLConfig 根据 MySQL 主从配置创建 RWDB
func CreateRWDBFromMySQLConfig(masterDSN string, slaveDSNs []string, poolConfig ...MySQLOption) (*RWDB, error) {
	master := NewMySQLAdapter(masterDSN, poolConfig...)
	if err := master.Connect(context.Background()); err != nil {
		return nil, fmt.Errorf("connect master: %w", err)
	}

	var slaves []*gorm.DB
	for i, dsn := range slaveDSNs {
		slave := NewMySQLAdapter(dsn, poolConfig...)
		if err := slave.Connect(context.Background()); err != nil {
			// 从库连接失败，记录但不阻塞启动
			master.GORM().Logger.Warn(context.Background(), "slave connect failed, skip", "index", i, "error", err)
			continue
		}
		slaves = append(slaves, slave.GORM())
	}

	return NewRWDB(master.GORM(), slaves), nil
}

// CreateRWDBFromPostgresConfig 根据 PostgreSQL 主从配置创建 RWDB
// poolOpts 为可选连接池参数（与 business 库配置对齐，需求 §6）；不传则走适配器默认值。
func CreateRWDBFromPostgresConfig(masterDSN string, slaveDSNs []string, poolOpts ...PostgreSQLOption) (*RWDB, error) {
	master := NewPostgreSQLAdapter(masterDSN, poolOpts...)
	if err := master.Connect(context.Background()); err != nil {
		return nil, fmt.Errorf("connect master: %w", err)
	}

	var slaves []*gorm.DB
	for i, dsn := range slaveDSNs {
		slave := NewPostgreSQLAdapter(dsn, poolOpts...)
		if err := slave.Connect(context.Background()); err != nil {
			master.GORM().Logger.Warn(context.Background(), "slave connect failed, skip", "index", i, "error", err)
			continue
		}
		slaves = append(slaves, slave.GORM())
	}

	return NewRWDB(master.GORM(), slaves), nil
}

// CreateSingleRWDB 创建单主库的 RWDB（读写不分库时使用）
func CreateSingleRWDB(db *gorm.DB) *RWDB {
	return NewRWDB(db, nil)
}

// ============================================
// slavePingLoop 从库健康检测（后台协程）
// ============================================

// SlavePingLoop 从库定期探活：标记不健康的从库为 unhealthy，恢复后重新加入
func (rw *RWDB) SlavePingLoop(ctx context.Context, interval time.Duration) {
	if len(rw.slaves) == 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rw.pingAndUpdateSlaves(ctx)
		}
	}
}

// pingAndUpdateSlaves 对所有从库执行 ping 并更新健康状态。
// 关键并发优化（高并发读写分离，需求 §6.3）：不再在阻塞式 ping 期间长期持有写锁。
// 旧实现：ping 每个从库（最长 2s）全程持 rw.mu 写锁，期间 pickSlave / Ping 的读锁被
// 串行阻塞，从库越多读路径延迟越高。
// 新实现：仅在「读锁」下快照从库列表，随后在锁外执行阻塞 ping，最后短暂持写锁原子替换
// 健康集合。读路径（pickSlave）几乎不再被探活阻塞，显著降低锁竞争。
func (rw *RWDB) pingAndUpdateSlaves(ctx context.Context) {
	// 1. 读锁下快照从库列表（rw.slaves 创建后不可变；unhealthySet 会被本周期结果整体替换）。
	rw.mu.RLock()
	slaves := make([]*gorm.DB, len(rw.slaves))
	copy(slaves, rw.slaves)
	rw.mu.RUnlock()

	// 2. 锁外执行阻塞式 ping（高并发下不再阻塞 pickSlave 的读锁）。
	unhealthy := make(map[*gorm.DB]bool, len(slaves))
	for _, s := range slaves {
		sqlDB, err := s.DB()
		if err != nil {
			unhealthy[s] = true
			continue
		}
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		pingErr := sqlDB.PingContext(pingCtx)
		cancel()
		if pingErr != nil {
			unhealthy[s] = true
		}
	}

	// 3. 短暂写锁，原子替换健康集合（避免与 pickSlave 的读锁长期互斥）。
	rw.mu.Lock()
	rw.unhealthySet = unhealthy
	rw.mu.Unlock()
}

// SlaveHealth 返回从库健康状态（用于监控）
func (rw *RWDB) SlaveHealth() map[int]bool {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	result := make(map[int]bool, len(rw.slaves))
	for i, s := range rw.slaves {
		result[i] = !rw.unhealthySet[s]
	}
	return result
}

// NewRWDBFromGORM 从单个 GORM DB 创建 RWDB（用于事务内复用 Repository 结构）
func NewRWDBFromGORM(db *gorm.DB) *RWDB {
	return NewRWDB(db, nil)
}
