package repository

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/cdcdx/hc-framework-go/internal/cache"
	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/metrics"
	"github.com/cdcdx/hc-framework-go/internal/model"
)

// IdleRepository 挂机记录仓库
type IdleRepository struct {
	rw                      *db.RWDB
	cacheMgr                *cache.Manager // 三级缓存（可选）
	activeSetShards         int            // 活跃会话集合分片数（集群分片用，<=0 退化为单集合）
	heartbeatPersistEnabled bool           // 心跳时间批量落库开关（去逐次 UPDATE）
}

// NewIdleRepository 创建挂机仓库（读写分离）
func NewIdleRepository(rw *db.RWDB) *IdleRepository {
	return &IdleRepository{rw: rw}
}

// NewIdleRepositoryWithCache 创建带缓存的挂机仓库
// activeSetShards 为活跃会话集合分片数（集群分片用，<=0 退化为单集合 idle:active:set:0）。
// heartbeatPersistEnabled 开启后，TouchHeartbeat 的降级路径不再逐次 UPDATE，改由后台批量 flush 维护 last_heartbeat_at。
func NewIdleRepositoryWithCache(rw *db.RWDB, cacheMgr *cache.Manager, activeSetShards int, heartbeatPersistEnabled bool) *IdleRepository {
	return &IdleRepository{rw: rw, cacheMgr: cacheMgr, activeSetShards: activeSetShards, heartbeatPersistEnabled: heartbeatPersistEnabled}
}

// NewIdleRepositoryFromTx 从事务 gorm.DB 创建挂机仓库（事务内使用，强制走主库，不使用缓存）
func NewIdleRepositoryFromTx(tx *gorm.DB) *IdleRepository {
	return &IdleRepository{rw: db.NewRWDBFromGORM(tx)}
}

// Create 创建挂机记录（写主库，INSERT ... ON CONFLICT 防并发重复）
func (r *IdleRepository) Create(ctx context.Context, record *model.IdleRecord) error {
	if r.rw == nil {
		return fmt.Errorf("idle repo: rw not initialized")
	}
	err := r.rw.Write(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "device_id"}, {Name: "status"}},
		Where:     clause.Where{Exprs: []clause.Expression{clause.Eq{Column: "status", Value: "active"}}},
		DoNothing: true,
	}).Create(record).Error
	if err == nil {
		// Start 幂等预检（FindActiveByDevice）会把「设备级」键（idle:active:{userID}:{deviceID}）
		// 负缓存为空值 30s；此处必须把设备级键一并失效，否则新建的 active 记录在 TTL 内
		// 仍被当作“不存在”，导致紧随其后的 stop-device 误判 not idle。
		r.invalidateActiveCache(ctx, record.UserID)
		r.invalidateDeviceCache(ctx, record.UserID, record.DeviceID)
	}
	return err
}

// FindActive 查询用户活跃挂机会话（读从库 → 三级缓存）
func (r *IdleRepository) FindActive(ctx context.Context, userID string) (*model.IdleRecord, error) {
	cacheKey := fmt.Sprintf("idle:active:%s", userID)
	return r.cachedFindOne(ctx, cacheKey, func() (*model.IdleRecord, error) {
		return r.findOneBy(ctx, "user_id = ? AND status = ?", userID, "active")
	})
}

// FindActiveByDevice 查询用户+设备的活跃会话（读从库 → 三级缓存）
func (r *IdleRepository) FindActiveByDevice(ctx context.Context, userID, deviceID string) (*model.IdleRecord, error) {
	cacheKey := fmt.Sprintf("idle:active:%s:%s", userID, deviceID)
	return r.cachedFindOne(ctx, cacheKey, func() (*model.IdleRecord, error) {
		return r.findOneBy(ctx, "user_id = ? AND device_id = ? AND status = ?", userID, deviceID, "active")
	})
}

// cachedFindOne 三级缓存读取单个 IdleRecord（30s TTL，命中即短路；miss 回源 DB）。
func (r *IdleRepository) cachedFindOne(ctx context.Context, cacheKey string, loader func() (*model.IdleRecord, error)) (*model.IdleRecord, error) {
	const ttl = 30 * time.Second
	if r.cacheMgr != nil {
		val, err := r.cacheMgr.Get(ctx, cacheKey, ttl, func(ctx context.Context) (interface{}, error) {
			return loader()
		})
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		return cache.DecodeCached[*model.IdleRecord](val)
	}
	return loader()
}

// findOneBy 按条件查询单个活跃记录，NotFound 时返回 nil。
func (r *IdleRepository) findOneBy(ctx context.Context, query string, args ...interface{}) (*model.IdleRecord, error) {
	if r.rw == nil {
		return nil, fmt.Errorf("idle repo: rw not initialized")
	}
	var record model.IdleRecord
	err := r.rw.Read(ctx).Where(query, args...).First(&record).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &record, nil
}

// FindActiveAll 查询用户所有活跃会话（读从库）
func (r *IdleRepository) FindActiveAll(ctx context.Context, userID string) ([]model.IdleRecord, error) {
	if r.rw == nil {
		return nil, fmt.Errorf("idle repo: rw not initialized")
	}
	var records []model.IdleRecord
	err := r.rw.Read(ctx).
		Where("user_id = ? AND status = ?", userID, "active").
		Order("start_time ASC").
		Find(&records).Error
	return records, err
}

// CountActive 统计用户活跃会话数（读从库）
func (r *IdleRepository) CountActive(ctx context.Context, userID string) (int64, error) {
	if r.rw == nil {
		return 0, fmt.Errorf("idle repo: rw not initialized")
	}
	var count int64
	err := r.rw.Read(ctx).Model(&model.IdleRecord{}).
		Where("user_id = ? AND status = ?", userID, "active").
		Count(&count).Error
	return count, err
}

// FindByID 根据 ID 查询（读从库 → 三级缓存，与 FindActive/FindActiveByDevice 同构）
func (r *IdleRepository) FindByID(ctx context.Context, id int64) (*model.IdleRecord, error) {
	cacheKey := fmt.Sprintf("idle:id:%d", id)
	return r.cachedFindOne(ctx, cacheKey, func() (*model.IdleRecord, error) {
		return r.findOneBy(ctx, "id = ?", id)
	})
}

// Update 更新记录（写主库）
func (r *IdleRepository) Update(ctx context.Context, record *model.IdleRecord) error {
	if r.rw == nil {
		return fmt.Errorf("idle repo: rw not initialized")
	}
	err := r.rw.Write(ctx).Save(record).Error
	if err == nil {
		r.invalidateActiveCache(ctx, record.UserID)
		r.invalidateDeviceCache(ctx, record.UserID, record.DeviceID)
	}
	return err
}

// ────────── Redis 心跳（判活标记） ──────────
// 心跳去 DB 化：高频心跳仅写入 Redis 带 TTL 的存活标记，不再写主库、不再失效缓存、不再广播。
// Key = idle:hb:{userID}:{deviceID}，TTL = timeout_threshold；心跳周期刷新该 Key。
// 判活（离线检测 / Status）以该 Key 是否存在且未过期为准。

const heartbeatKeyFmt = "idle:hb:%s:%s"

// activeSetKeyFmt 活跃会话集合（离线检测 scanner 遍历此集合而非扫 DB）。
// 方案 3（集群分片）：集合按 userID 哈希分片为多个小集合（idle:active:set:{shard}），
// 集群模式下可打散到不同 slot/节点，避免单一大 key 成为热点；单机模式无害。
// 成员格式：userID<0x1f>deviceID（以不可见于业务 ID 的分隔符拼接，避免歧义）。
const activeSetKeyFmt = "idle:active:set:%d"
const activeMemberSep = "\x1f"

func activeMember(userID, deviceID string) string {
	return userID + activeMemberSep + deviceID
}

// activeSetShard 计算 userID 所属分片（心跳 key 已含 userID，自然分散；集合分片补齐同维度）。
func (r *IdleRepository) activeSetShard(userID string) int {
	shards := r.activeSetShards
	if shards <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(userID))
	return int(h.Sum32() % uint32(shards))
}

// activeSetKeyOf 返回某 userID 对应的分片集合 key
func (r *IdleRepository) activeSetKeyOf(userID string) string {
	return fmt.Sprintf(activeSetKeyFmt, r.activeSetShard(userID))
}

// activeSetShardCount 返回分片总数（<=0 时为 1，即单集合）
func (r *IdleRepository) activeSetShardCount() int {
	if r.activeSetShards <= 0 {
		return 1
	}
	return r.activeSetShards
}

// ActiveSetShardOf 返回某 userID 所属活跃集合分片序号（导出封装 activeSetShard，供 service 层分片判断）。
func (r *IdleRepository) ActiveSetShardOf(userID string) int {
	return r.activeSetShard(userID)
}

func ParseActiveMember(m string) (userID, deviceID string) {
	parts := strings.SplitN(m, activeMemberSep, 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", ""
}

// ParseHeartbeatKey 从完整 Redis key（含 keyPrefix）解析出 userID/deviceID。
// 心跳 key 格式：{keyPrefix}:idle:hb:{userID}:{deviceID}；此处按 "idle:hb:" 标记定位，与 keyPrefix 解耦。
// 用于 Keyspace Notification 的 expired 事件回调（payload 为完整 key 名）。
func ParseHeartbeatKey(fullKey string) (userID, deviceID string, ok bool) {
	const marker = "idle:hb:"
	idx := strings.Index(fullKey, marker)
	if idx < 0 {
		return "", "", false
	}
	rest := fullKey[idx+len(marker):]
	parts := strings.SplitN(rest, ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// heartbeatRedisEnabled 是否启用 Redis 心跳判活（无 Redis 时降级为 DB last_heartbeat_at）
func (r *IdleRepository) heartbeatRedisEnabled() bool {
	return r.cacheMgr != nil && r.cacheMgr.L2 != nil
}

// HeartbeatRedisEnabled 暴露给 service 层，用于选择离线检测 scanner 路径
func (r *IdleRepository) HeartbeatRedisEnabled() bool {
	return r.heartbeatRedisEnabled()
}

// TouchHeartbeat 上报心跳：Redis 可用时仅 SETEX 存活标记（不写主库、不失效、不广播）；
// Redis 不可用时降级写主库 last_heartbeat_at（同样不触发缓存失效与广播）。
func (r *IdleRepository) TouchHeartbeat(ctx context.Context, userID, deviceID string, ttl time.Duration) error {
	if r.heartbeatRedisEnabled() {
		key := fmt.Sprintf(heartbeatKeyFmt, userID, deviceID)
		return r.cacheMgr.L2.Set(ctx, key, time.Now().Unix(), ttl)
	}
	// 降级：Redis 不可用时判活依据 DB last_heartbeat_at。
	// 若启用了批量落库（HeartbeatPersistEnabled），last_heartbeat_at 由后台 HeartbeatFlusher
	// 聚合批量维护，这里不再逐次 UPDATE（避免高频 DB 写）；否则回退为旧逐次 UPDATE 兜底。
	if r.heartbeatPersistEnabled {
		return nil
	}
	return r.updateHeartbeatDB(ctx, userID, deviceID)
}

// updateHeartbeatDB 仅更新主库 last_heartbeat_at，不做任何缓存失效/广播（降级路径使用）
func (r *IdleRepository) updateHeartbeatDB(ctx context.Context, userID, deviceID string) error {
	if r.rw == nil {
		return fmt.Errorf("idle repo: rw not initialized")
	}
	now := time.Now()
	return r.rw.Write(ctx).Model(&model.IdleRecord{}).
		Where("user_id = ? AND device_id = ? AND status = ?", userID, deviceID, "active").
		Update("last_heartbeat_at", &now).Error
}

// DeleteHeartbeat 清理心跳标记（会话结算/停止时调用）
func (r *IdleRepository) DeleteHeartbeat(ctx context.Context, userID, deviceID string) error {
	if !r.heartbeatRedisEnabled() {
		return nil
	}
	return r.cacheMgr.L2.Delete(ctx, fmt.Sprintf(heartbeatKeyFmt, userID, deviceID))
}

// IsHeartbeatAlive 判断会话是否存活（Redis 心跳 Key 是否存在且未过期）
// 仅在 Redis 可用时调用（heartbeatRedisEnabled 为 true）。
func (r *IdleRepository) IsHeartbeatAlive(ctx context.Context, userID, deviceID string) (bool, error) {
	return r.cacheMgr.L2.Exists(ctx, fmt.Sprintf(heartbeatKeyFmt, userID, deviceID))
}

// BatchHeartbeatAlive 批量判定活跃集合成员的存活状态，返回「心跳 Key 已缺失/过期」的成员
// （保持与 active 集合成员相同的格式：userID<0x1f>deviceID）。基于 L2.BatchExists（Pipeline 批量），
// 将离线检测 scanner 的判活 RTT 从 O(N) 降到 O(N/batch)——百万活跃会话场景下从百万次 RTT 降至数百次。
// 仅在 Redis 可用时调用。
func (r *IdleRepository) BatchHeartbeatAlive(ctx context.Context, members []string) ([]string, error) {
	if !r.heartbeatRedisEnabled() {
		return nil, fmt.Errorf("redis unavailable")
	}
	if len(members) == 0 {
		return nil, nil
	}
	keys := make([]string, len(members))
	for i, m := range members {
		u, d := ParseActiveMember(m)
		if u == "" {
			keys[i] = "" // 无法解析，视为非活跃，后续由 scanner 清理
			continue
		}
		keys[i] = fmt.Sprintf(heartbeatKeyFmt, u, d)
	}
	alive, err := r.cacheMgr.L2.BatchExists(ctx, keys)
	if err != nil {
		return nil, err
	}
	dead := make([]string, 0, len(members)/10)
	for i, ok := range alive {
		if ok || keys[i] == "" {
			continue
		}
		dead = append(dead, members[i])
	}
	return dead, nil
}

// GetHeartbeatTS 获取最近一次心跳的 Unix 秒级时间戳（用于 Status 展示实时心跳）。
// Redis 不可用时返回 0。
func (r *IdleRepository) GetHeartbeatTS(ctx context.Context, userID, deviceID string) (int64, error) {
	if !r.heartbeatRedisEnabled() {
		return 0, nil
	}
	val, ok, err := r.cacheMgr.L2.Get(ctx, fmt.Sprintf(heartbeatKeyFmt, userID, deviceID))
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	switch v := val.(type) {
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	default:
		return 0, nil
	}
}

// BatchGetHeartbeatTS 批量获取心跳时间戳（使用 Redis Pipeline 消除 N 次往返）。
// 返回 map[deviceID]timestamp，按 deviceID 索引。Redis 不可用时返回空 map。
func (r *IdleRepository) BatchGetHeartbeatTS(ctx context.Context, userID string, deviceIDs []string) (map[string]int64, error) {
	result := make(map[string]int64, len(deviceIDs))
	if !r.heartbeatRedisEnabled() || len(deviceIDs) == 0 {
		return result, nil
	}
	pipe, err := r.cacheMgr.L2.Pipeline(ctx)
	if err != nil {
		return result, err
	}
	for _, deviceID := range deviceIDs {
		_ = pipe.Get(ctx, fmt.Sprintf(heartbeatKeyFmt, userID, deviceID))
	}
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		return result, err
	}
	for i, cmd := range cmds {
		if i >= len(deviceIDs) {
			break
		}
		// Pipeline 的 Get 结果从 *redis.StringCmd 中提取
		if strCmd, ok := cmd.(interface{ Val() string }); ok {
			val := strCmd.Val()
			if val != "" {
				if ts, err := strconv.ParseFloat(val, 64); err == nil && ts > 0 {
					result[deviceIDs[i]] = int64(ts)
				}
			}
		}
	}
	return result, nil
}

// HeartbeatPersistItem 单次心跳聚合项（已由 HeartbeatFlusher 按 user+device 去重，TS 为该会话最新心跳时间）。
type HeartbeatPersistItem struct {
	UserID   string
	DeviceID string
	TS       time.Time
}

// BatchUpdateHeartbeat 批量更新活跃会话的 last_heartbeat_at（单条 SQL 内批量 UPDATE，
// 仅 status='active' 时生效）。用于 HeartbeatFlusher 的定时聚合落库，替代逐次 UPDATE：
// DB 写压力从「每次心跳一次」降为「每 flush 间隔一次」，且同会话多次心跳只保留最新值。
// 已结算（status!=active）的会话自动被 WHERE 条件跳过，不会被错误回写。
//
// 优化：使用 CASE WHEN 构建单条 SQL 批量更新，替代事务内 N 次逐条 UPDATE 的 N 次网络往返。
func (r *IdleRepository) BatchUpdateHeartbeat(ctx context.Context, items []HeartbeatPersistItem) (int, error) {
	if r.rw == nil {
		return 0, fmt.Errorf("idle repo: rw not initialized")
	}
	if len(items) == 0 {
		return 0, nil
	}

	// 构建 CASE WHEN 条件与 WHERE 条件
	var (
		tsCases    []string
		tsArgs     []interface{}
		whereCases []string
		whereArgs  []interface{}
	)

	for _, it := range items {
		tsCases = append(tsCases, "WHEN (user_id = ? AND device_id = ?) THEN ?")
		tsArgs = append(tsArgs, it.UserID, it.DeviceID, it.TS)
		whereCases = append(whereCases, "(user_id = ? AND device_id = ? AND status = 'active')")
		whereArgs = append(whereArgs, it.UserID, it.DeviceID)
	}

	now := time.Now()
	sql := fmt.Sprintf(
		"UPDATE idle_records SET last_heartbeat_at = CASE %s ELSE last_heartbeat_at END, updated_at = ? WHERE %s",
		strings.Join(tsCases, " "),
		strings.Join(whereCases, " OR "),
	)

	args := append(tsArgs, now)
	args = append(args, whereArgs...)
	result := r.rw.Write(ctx).Exec(sql, args...)
	if result.Error != nil {
		return 0, result.Error
	}
	return int(result.RowsAffected), nil
}

// AddActiveSession 将活跃会话登记到 Redis 分片集合（离线检测 scanner 遍历该集合而非扫 DB）。
// 按 userID 哈希分片，集群模式下分散到不同 slot/节点。Redis 不可用时为 no-op。
func (r *IdleRepository) AddActiveSession(ctx context.Context, userID, deviceID string) error {
	if !r.heartbeatRedisEnabled() {
		return nil
	}
	return r.cacheMgr.L2.SAdd(ctx, r.activeSetKeyOf(userID), activeMember(userID, deviceID))
}

// RemoveActiveSession 从 Redis 分片集合移除会话（结算/停止时调用）。
func (r *IdleRepository) RemoveActiveSession(ctx context.Context, userID, deviceID string) error {
	if !r.heartbeatRedisEnabled() {
		return nil
	}
	return r.cacheMgr.L2.SRem(ctx, r.activeSetKeyOf(userID), activeMember(userID, deviceID))
}

// ScanActiveSessions 返回所有分片集合内的活跃会话成员（userID<0x1f>deviceID）。
// 集群模式下遍历各分片集合（SSCAN 自动翻页），将离线检测负载分散到多节点。
// 仅在 Redis 可用时调用（heartbeatRedisEnabled 为 true）。
func (r *IdleRepository) ScanActiveSessions(ctx context.Context) ([]string, error) {
	if !r.heartbeatRedisEnabled() {
		return nil, fmt.Errorf("redis unavailable")
	}
	var out []string
	shards := r.activeSetShardCount()
	for i := 0; i < shards; i++ {
		members, err := r.cacheMgr.L2.SScan(ctx, fmt.Sprintf(activeSetKeyFmt, i))
		if err != nil {
			return out, err
		}
		out = append(out, members...)
	}
	return out, nil
}

// ScanActiveSessionsInShards 仅扫描指定分片序号集合（离线检测按 Pod 分片用）。
// 每个 Pod 只遍历属于自己的 set-shard（按 i % pod_total == pod_index 分配），
// 将全量 O(N) 扫描负载分散到多个 Pod，消除“每副本全量”的冗余。
// 仅在 Redis 可用时调用。
func (r *IdleRepository) ScanActiveSessionsInShards(ctx context.Context, shardIdxs []int) ([]string, error) {
	if !r.heartbeatRedisEnabled() {
		return nil, fmt.Errorf("redis unavailable")
	}
	var out []string
	for _, i := range shardIdxs {
		members, err := r.cacheMgr.L2.SScan(ctx, fmt.Sprintf(activeSetKeyFmt, i))
		if err != nil {
			return out, err
		}
		out = append(out, members...)
	}
	return out, nil
}

// ActiveSetShardCount 返回活跃集合的分片总数（<=0 时为 1，即单集合）。供 service 层计算 Pod 分片分配。
func (r *IdleRepository) ActiveSetShardCount() int {
	return r.activeSetShardCount()
}

// FindAllActive 查询所有活跃挂机会话（Redis 判活 scanner 使用）
// 与 FindStaleActive 不同：不依赖 DB 的 last_heartbeat_at（心跳已去 DB 化），
// 由上层逐个校验 Redis 心跳 Key 判定是否超时。百万级场景应改为 Redis 原生扫描（方案 4）。
func (r *IdleRepository) FindAllActive(ctx context.Context) ([]model.IdleRecord, error) {
	if r.rw == nil {
		return nil, fmt.Errorf("idle repo: rw not initialized")
	}
	var records []model.IdleRecord
	err := r.rw.Read(ctx).
		Where("status = ?", "active").
		Order("start_time ASC").
		Find(&records).Error
	return records, err
}

// FindStaleActive 查询心跳超时的活跃挂机会话（DB 降级 scanner 使用）
// before 通常为 now - timeoutThreshold；返回 last_heartbeat_at < before
// （或从未上报心跳且 start_time < before）且 status='active' 的记录。
func (r *IdleRepository) FindStaleActive(ctx context.Context, before time.Time) ([]model.IdleRecord, error) {
	if r.rw == nil {
		return nil, fmt.Errorf("idle repo: rw not initialized")
	}
	var records []model.IdleRecord
	err := r.rw.Read(ctx).
		Where("status = ? AND (last_heartbeat_at < ? OR (last_heartbeat_at IS NULL AND start_time < ?))",
			"active", before, before).
		Order("start_time ASC").
		Find(&records).Error
	return records, err
}

// CompleteIfActive 仅在记录仍为 active 时标记完成（乐观锁，防止多实例重复结算）。
// 返回 true 表示实际更新了行（本次结算生效）。
func (r *IdleRepository) CompleteIfActive(ctx context.Context, id int64, endTime time.Time, durationSeconds int, pointsEarned int64) (bool, error) {
	return r.markSettled(ctx, id, "completed", endTime, durationSeconds, pointsEarned)
}

// TimeoutIfActive 仅在记录仍为 active 时标记超时（乐观锁，防止多实例重复结算）。
// 返回 true 表示实际更新了行（本次结算生效）。
func (r *IdleRepository) TimeoutIfActive(ctx context.Context, id int64, endTime time.Time, durationSeconds int, pointsEarned int64) (bool, error) {
	return r.markSettled(ctx, id, "timeout", endTime, durationSeconds, pointsEarned)
}

// markSettled 原子地将 active 会话标记为终态，仅当 status='active' 时生效。
func (r *IdleRepository) markSettled(ctx context.Context, id int64, status string, endTime time.Time, durationSeconds int, pointsEarned int64) (bool, error) {
	if r.rw == nil {
		return false, fmt.Errorf("idle repo: rw not initialized")
	}
	result := r.rw.Write(ctx).Model(&model.IdleRecord{}).
		Where("id = ? AND status = ?", id, "active").
		Updates(map[string]interface{}{
			"end_time":         endTime,
			"duration_seconds": durationSeconds,
			"points_earned":    pointsEarned,
			"status":           status,
		})
	if result.Error != nil {
		return false, result.Error
	}
	// 精确的按用户缓存失效由上层 idle_service.settleSession 在事务提交后
	// 调用 InvalidateActiveByUser 完成（key 为 idle:active:{userID} / idle:active:{userID}:{deviceID}）。
	// 此处不再调用失效逻辑，避免删除与读取 key 不匹配的常量键（无效且易误导）。
	return result.RowsAffected > 0, nil
}

// ────────── 缓存失效辅助 ──────────

func (r *IdleRepository) invalidateActiveCache(ctx context.Context, userID string) {
	if r.cacheMgr == nil {
		return
	}
	_ = r.cacheMgr.Delete(ctx, fmt.Sprintf("idle:active:%s", userID))
}

func (r *IdleRepository) invalidateDeviceCache(ctx context.Context, userID, deviceID string) {
	if r.cacheMgr == nil {
		return
	}
	_ = r.cacheMgr.Delete(ctx, fmt.Sprintf("idle:active:%s:%s", userID, deviceID))
}

// InvalidateActiveByUser 精确失效某用户的活跃挂机缓存（含设备维度）
// 读取路径按 idle:active:{userID} / idle:active:{userID}:{deviceID} 缓存，
// 写路径必须用完全相同的 key 失效，否则会残留脏数据直到 TTL 过期。
// 结算路径（idle_service.settleSession）在事务提交后调用本方法，确保不会返回已结算的脏状态。
func (r *IdleRepository) InvalidateActiveByUser(ctx context.Context, userID, deviceID string) {
	if r.cacheMgr == nil {
		return
	}
	keys := []string{fmt.Sprintf("idle:active:%s", userID)}
	if deviceID != "" {
		keys = append(keys, fmt.Sprintf("idle:active:%s:%s", userID, deviceID))
	}
	_ = r.cacheMgr.Delete(ctx, keys...)
}

// GetDailyPoints 获取用户当日已累计的挂机积分。
// 优先读 Redis 计数器（O(1)，由 IncrDailyPoints 维护）；未命中（冷启动/Redis 故障）回退 DB SUM 并回填。
// L2 不可用时直接 DB SUM。百万级结算下，把「每次结算一次全表 SUM 扫描」降为「O(1) 读取 + 偶发回填」。
func (r *IdleRepository) GetDailyPoints(ctx context.Context, userID string) (int64, error) {
	if r.cacheMgr != nil && r.cacheMgr.L2 != nil {
		if v, ok, err := r.cacheMgr.L2.Get(ctx, r.dailyPointsKey(userID)); err == nil && ok {
			if n, ok := toInt64(v); ok {
				return n, nil
			}
		}
		// 冷启动/缓存未命中：从 DB 计算并回填，后续结算直接走 Redis 计数（TTL 至当日结束+1h）。
		dbVal, dbErr := r.getDailyPointsDB(ctx, userID)
		if dbErr == nil {
			_ = r.cacheMgr.L2.Set(ctx, r.dailyPointsKey(userID), dbVal, endOfLocalDay())
		} else {
			// DB 冷启动回源失败：Redis 计数无法预热，但结算主流程仍可走 DB SUM。
			// 仅记指标不抛错；持续上升表示 DB 抖动。
			metrics.IdleRepoErrorsTotal.WithLabelValues("get_daily_points_db").Inc()
		}
		return dbVal, dbErr
	}
	return r.getDailyPointsDB(ctx, userID)
}

// getDailyPointsDB 读取用户当日已得挂机积分（降级/回填用）。
// 改为读 idle_daily_points 单行汇总（O(1)），彻底去掉对 idle_records 的 SUM 范围扫描。
// 汇总行不存在（历史数据/新用户首笔）时回退一次全表 SUM 并回填汇总行，使后续回源均为 O(1)，
// 把「每次结算一次全表 SUM」收敛为「每用户每天至多一次 SUM」（根治 p99 长尾的隐性放大器）。
func (r *IdleRepository) getDailyPointsDB(ctx context.Context, userID string) (int64, error) {
	day := LocalDayString()
	if v, ok, err := r.readDailyPointsSummary(ctx, userID, day); err == nil {
		if ok {
			return v, nil
		}
		// 汇总行缺失：回退全表 SUM 并回填（一次性），避免后续每次回源都扫全表。
		dbVal, dbErr := r.sumDailyPointsDB(ctx, userID)
		if dbErr == nil {
			_ = r.upsertDailyPointsSummary(ctx, userID, day, dbVal)
		}
		return dbVal, dbErr
	}
	// 汇总表读失败：降级为全表 SUM（保留旧行为，不阻断结算）。
	return r.sumDailyPointsDB(ctx, userID)
}

// LocalDayString 返回本地时区当日日期串 "2006-01-02"，与 Redis 每日计数器边界（endOfLocalDay）一致。
// 导出供 service 层在事务内维护汇总时复用同一日边界。
func LocalDayString() string {
	return time.Now().Format("2006-01-02")
}

// UpsertDailyPoints 在业务事务内增量累加用户当日汇总积分（total += delta），
// 与积分流水/Outbox 同事务提交；根治 getDailyPointsDB 的全表 SUM：改「读单行汇总」为「写时增量维护」。
// 用 ON CONFLICT DO UPDATE total = total + ? 实现 mysql/postgres/sqlite 跨库幂等增量。
func (r *IdleRepository) UpsertDailyPoints(ctx context.Context, userID, day string, delta int64) error {
	if r.rw == nil {
		return fmt.Errorf("idle repo: rw not initialized")
	}
	if delta <= 0 {
		return nil
	}
	row := &model.IdleDailyPoints{UserID: userID, Day: day, Total: delta}
	return r.rw.Write(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "day"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"total": gorm.Expr("total + ?", delta)}),
	}).Create(row).Error
}

// readDailyPointsSummary 读当日汇总单行（O(1)）；行不存在返回 (0,false,nil)。
func (r *IdleRepository) readDailyPointsSummary(ctx context.Context, userID, day string) (int64, bool, error) {
	if r.rw == nil {
		return 0, false, fmt.Errorf("idle repo: rw not initialized")
	}
	var row model.IdleDailyPoints
	err := r.rw.Read(ctx).Model(&model.IdleDailyPoints{}).
		Where("user_id = ? AND day = ?", userID, day).
		First(&row).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return 0, false, nil
		}
		return 0, false, err
	}
	return row.Total, true, nil
}

// sumDailyPointsDB 旧实现：对 idle_records 做当日全量 SUM（范围扫描，慢）；仅作汇总行缺失时的回退/回填。
func (r *IdleRepository) sumDailyPointsDB(ctx context.Context, userID string) (int64, error) {
	if r.rw == nil {
		return 0, fmt.Errorf("idle repo: rw not initialized")
	}
	today := time.Now().Truncate(24 * time.Hour)
	var total int64
	err := r.rw.Read(ctx).Model(&model.IdleRecord{}).
		Where("user_id = ? AND created_at >= ?", userID, today).
		Select("COALESCE(SUM(points_earned), 0)").
		Scan(&total).Error
	return total, err
}

// upsertDailyPointsSummary 回填汇总行（SET 非增量，用于历史数据一次性回源后持久化，幂等）。
func (r *IdleRepository) upsertDailyPointsSummary(ctx context.Context, userID, day string, total int64) error {
	if r.rw == nil {
		return fmt.Errorf("idle repo: rw not initialized")
	}
	row := &model.IdleDailyPoints{UserID: userID, Day: day, Total: total}
	return r.rw.Write(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "day"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"total": total}),
	}).Create(row).Error
}

// BackfillDailyPoints 批量回填当日积分汇总（根治 getDailyPointsDB 的日初全表 SUM 风暴）。
// 用单条 INSERT...SELECT GROUP BY 一次性算出所有用户当日总额并写入 idle_daily_points，
// 替代「每个用户首笔查询各做一次全表 SUM」的 N 次范围扫描；之后读路径均走单行 O(1)。
// ON CONFLICT DO NOTHING 保证仅补「缺失行」，不覆盖结算时增量维护的 running total（幂等、可重复执行）。
// dayStart 与 sumDailyPointsDB 保持一致（time.Now().Truncate(24h)），确保回填口径与回退 SUM 一致。
func (r *IdleRepository) BackfillDailyPoints(ctx context.Context, day string) (int64, error) {
	if r.rw == nil {
		return 0, fmt.Errorf("idle repo: rw not initialized")
	}
	dayStart := time.Now().Truncate(24 * time.Hour)
	master := r.rw.Master()
	var sql string
	switch master.Dialector.Name() {
	case "mysql":
		sql = `INSERT INTO idle_daily_points (user_id, day, total)
			SELECT user_id, ? as day, COALESCE(SUM(points_earned), 0) as total
			FROM idle_records
			WHERE created_at >= ?
			GROUP BY user_id
			ON DUPLICATE KEY UPDATE total = total`
	case "postgres":
		sql = `INSERT INTO idle_daily_points (user_id, day, total)
			SELECT user_id, ? as day, COALESCE(SUM(points_earned), 0) as total
			FROM idle_records
			WHERE created_at >= ?
			GROUP BY user_id
			ON CONFLICT (user_id, day) DO NOTHING`
	default: // sqlite
		sql = `INSERT OR IGNORE INTO idle_daily_points (user_id, day, total)
			SELECT user_id, ? as day, COALESCE(SUM(points_earned), 0) as total
			FROM idle_records
			WHERE created_at >= ?
			GROUP BY user_id`
	}
	res := master.WithContext(ctx).Exec(sql, day, dayStart)
	if res.Error != nil {
		return 0, res.Error
	}
	return res.RowsAffected, nil
}

// IncrDailyPoints 结算生效后累加当日积分计数器（best-effort；key 已由 GetDailyPoints 预热并带 TTL）。
// 失败仅记日志忽略，不影响结算结果（积分余额以 users 表为准）。
func (r *IdleRepository) IncrDailyPoints(ctx context.Context, userID string, pts int64) {
	if r.cacheMgr == nil || r.cacheMgr.L2 == nil || pts <= 0 {
		return
	}
	if _, err := r.cacheMgr.L2.IncrBy(ctx, r.dailyPointsKey(userID), pts); err != nil {
		zap.L().Warn("incr daily points counter failed", zap.String("user_id", userID), zap.Error(err))
		// Redis 计数累加失败不影响结算（积分余额以 users 表为准），但持续失败说明 L2 抖动，需可观测。
		metrics.IdleRepoErrorsTotal.WithLabelValues("incr_daily_points").Inc()
	}
}

// AcquireDailyPoints 原子预占当日挂机积分额度（封顶到 limit），修复结算 TOCTOU。
// 预占前先确保 Redis 计数已预热（反映当日 DB 实际累计），避免冷启动从 0 封顶导致超发。
// 返回 granted：实际授予额度（已计入 Redis 计数）。余额以 users 表为准，Redis 仅作限额跟踪。
func (r *IdleRepository) AcquireDailyPoints(ctx context.Context, userID string, want, limit int64) (int64, error) {
	if r.cacheMgr == nil || r.cacheMgr.L2 == nil || limit <= 0 {
		return want, nil
	}
	// 预热：L2 中计数不存在时，先从 DB SUM 回填，使预占基于真实已发放量。
	if _, ok, _ := r.cacheMgr.L2.Get(ctx, r.dailyPointsKey(userID)); !ok {
		_, _ = r.GetDailyPoints(ctx, userID)
	}
	return r.cacheMgr.TryAcquireDailyPoints(ctx, userID, want, limit)
}

// ReleaseDailyPoints 回退预占额度（事务失败或被乐观锁跳过时调用）。
func (r *IdleRepository) ReleaseDailyPoints(ctx context.Context, userID string, granted int64) error {
	if r.cacheMgr == nil || r.cacheMgr.L2 == nil || granted <= 0 {
		return nil
	}
	return r.cacheMgr.ReleaseDailyPoints(ctx, userID, granted)
}

// dailyPointsKey 当日已得挂机积分计数器（Redis）。Key 命名委托 cache.IdleDailyPointsKey，保证单一来源。
func (r *IdleRepository) dailyPointsKey(userID string) string {
	return cache.IdleDailyPointsKey(userID)
}

// toInt64 将缓存反序列化结果（float64/int64/string）安全转为 int64。
func toInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case float64:
		return int64(n), true
	case int:
		return int64(n), true
	case string:
		if i, err := strconv.ParseInt(n, 10, 64); err == nil {
			return i, true
		}
	}
	return 0, false
}

// endOfLocalDay 返回距本地时区当日 24:00 的剩余时长（+1h 缓冲，用于每日计数器 TTL）。
func endOfLocalDay() time.Duration {
	now := time.Now()
	end := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()).Add(24 * time.Hour)
	return end.Sub(now) + time.Hour
}

// FindRecords 查询挂机记录（读从库，游标分页）
func (r *IdleRepository) FindRecords(ctx context.Context, userID string, cursor int64, limit int) ([]model.IdleRecord, error) {
	if r.rw == nil {
		return nil, fmt.Errorf("idle repo: rw not initialized")
	}
	if limit <= 0 {
		limit = 20
	}
	var records []model.IdleRecord
	query := r.rw.Read(ctx).
		Where("user_id = ?", userID).
		Order("id DESC").
		Limit(limit + 1)

	if cursor > 0 {
		query = query.Where("id < ?", cursor)
	}

	if err := query.Find(&records).Error; err != nil {
		return nil, err
	}
	return records, nil
}

// AutoMigrate 自动迁移（DDL 操作主库）
func (r *IdleRepository) AutoMigrate() error {
	if r.rw == nil {
		return fmt.Errorf("idle repo: rw not initialized")
	}
	return r.rw.Master().AutoMigrate(&model.IdleRecord{})
}

// EnsureUniqueIndex 确保同用户同设备只能有一个 active 会话（DDL 操作主库）
// SQLite: 使用部分唯一索引 (WHERE status='active')
// MySQL:   不支持部分唯一索引，使用生成列模拟：当 status='active' 时生成唯一键，非 active 时为 NULL（MySQL UNIQUE 索引允许多个 NULL）
func (r *IdleRepository) EnsureUniqueIndex() error {
	if r.rw == nil {
		return fmt.Errorf("idle repo: rw not initialized")
	}
	switch r.rw.Master().Dialector.Name() {
	case "mysql":
		master := r.rw.Master()
		// 1. 先检查列是否存在，不存在才添加（避免 GORM 输出 "Duplicate column" 错误日志）
		var colExists int64
		master.Raw("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'idle_records' AND COLUMN_NAME = 'active_device_key'").Scan(&colExists)
		if colExists == 0 {
			if err := master.Exec(`
				ALTER TABLE idle_records 
				ADD COLUMN active_device_key VARCHAR(191) 
				GENERATED ALWAYS AS (IF(status = 'active', CONCAT(user_id, ':', device_id), NULL)) STORED
			`).Error; err != nil {
				return err
			}
		}

		// 2. 先检查索引是否存在，不存在才创建
		var idxExists int64
		master.Raw("SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'idle_records' AND INDEX_NAME = 'idx_unique_active_device'").Scan(&idxExists)
		if idxExists == 0 {
			if err := master.Exec(`
				CREATE UNIQUE INDEX idx_unique_active_device 
				ON idle_records(active_device_key)
			`).Error; err != nil {
				return err
			}
		}
		return nil

	default: // sqlite 支持 IF NOT EXISTS + WHERE 部分索引
		return r.rw.Master().Exec(`
			CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_active_device 
			ON idle_records(user_id, device_id, status) 
			WHERE status = 'active'
		`).Error
	}
}
