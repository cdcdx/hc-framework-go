package common

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/repository"
	"github.com/cdcdx/hc-framework-go/pkg/contextkeys"
)

// 异步批量写入参数：有界缓冲 + 后台批量刷新。
// 设计目标：
//   - 主链路（注册/登录/结算等）写日志与监控指标时永不阻塞（channel 满即丢弃并计数告警）；
//   - 后端（ClickHouse/ES）不可用或过载时，不再无限 spawn goroutine 拖垮进程与连接池；
//   - 攒批批量写（ClickHouse 逐行 INSERT 是致命反模式，批量可数量级降低 part 数与超时）。
const (
	logBufferSize    = 8192            // 审计日志缓冲队列容量
	metricBufferSize = 8192            // 监控指标缓冲队列容量
	flushBatchSize   = 500             // 达到该条数立即刷新
	flushInterval    = 1 * time.Second // 未达批量阈值时的最大驻留时长
	flushTimeout     = 10 * time.Second
)

// LogService 统一日志/监控服务（异步有界缓冲 + 批量刷新）
// 合并说明：登录审计（含 login_type / login_result / fail_reason / device_info 结构化字段）
// 与原审计日志同落 audit_logs 一张表，不再单独维护 login_records，登录写入从 2 次降到 1 次。
type LogService struct {
	logRepo     repository.LogRepo
	monitorRepo repository.MonitorRepo

	logCh    chan *model.AuditLog
	metricCh chan *model.MonitorMetric

	droppedLogs    atomic.Int64 // 因缓冲满被丢弃的审计日志数
	droppedMetrics atomic.Int64 // 因缓冲满被丢弃的监控指标数

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// idSeq 进程内自增 ID 序列，用于给审计日志/监控指标生成主键。
// 背景：GORM 侧 ID=0 会走自增，但 ClickHouse 的 id(Int64) 没有默认值，
// 调用方不赋值会全写成 0（监控面板按 id 排查时会看到一堆 0）。这里在写入前统一赋值。
var idSeq int64

// nextID 返回进程内单调递增的唯一 int64（同一进程内不重复；跨进程无需全局唯一，
// 因为 id 仅为分析列、ClickHouse 不对其做唯一约束）。
func nextID() int64 {
	return atomic.AddInt64(&idSeq, 1)
}

// NewLogService 创建日志服务并启动后台批量写入协程。
func NewLogService(logRepo repository.LogRepo, monitorRepo repository.MonitorRepo) *LogService {
	s := &LogService{
		logRepo:     logRepo,
		monitorRepo: monitorRepo,
		logCh:       make(chan *model.AuditLog, logBufferSize),
		metricCh:    make(chan *model.MonitorMetric, metricBufferSize),
		stop:        make(chan struct{}),
	}
	s.wg.Add(2)
	go s.logLoop()
	go s.metricLoop()
	return s
}

// Close 优雅停止：停止接收后 drain 缓冲并刷新剩余数据。由 main 在退出流程中调用。
func (s *LogService) Close() {
	s.stopOnce.Do(func() { close(s.stop) })
	s.wg.Wait()
}

// EventMeta 事件元信息
type EventMeta struct {
	UserID    string
	IPAddress string
	UserAgent string
}

// LogRegister 注册日志
func (s *LogService) LogRegister(ctx context.Context, meta EventMeta, email string) {
	detail, _ := json.Marshal(map[string]string{"email": email})
	s.writeLog(ctx, "register", meta, string(detail), "", "", "", "")
	s.writeMetric(ctx, meta.UserID, "register", "register_count", 1, nil)
}

// LogLogin 登录日志（合并 login_records：登录审计与结构化登录字段同落 audit_logs 一行，
// 登录写入从 2 次降到 1 次；跟随 log.driver，ES 不可用时由 bootstrap 回退 SQLite）。
func (s *LogService) LogLogin(ctx context.Context, meta EventMeta, loginType, result, email, failReason, deviceInfo string) {
	detail, _ := json.Marshal(map[string]interface{}{
		"email":   email,
		"success": result == model.LoginResultSuccess,
	})
	s.writeLog(ctx, "login", meta, string(detail), loginType, result, failReason, deviceInfo)

	// 成功/失败都写指标，且 metric_type/metric_name 均用计数器名（login_count/login_fail_count），
	// 便于 MonitorSumByType 直接按计数器求和，失败率 = login_fail_count/(login_count+login_fail_count)，
	// 无需回查 audit_logs（SumByType 按 metric_type 过滤，故此处以计数器名为 metric_type）。
	counter := "login_count"
	if result != model.LoginResultSuccess {
		counter = "login_fail_count"
	}
	s.writeMetric(ctx, meta.UserID, counter, counter, 1, nil)
}

// LogDeviceOnline 设备上线日志
func (s *LogService) LogDeviceOnline(ctx context.Context, meta EventMeta, deviceID string) {
	detail, _ := json.Marshal(map[string]string{"device_id": deviceID})
	s.writeLog(ctx, "device_online", meta, string(detail), "", "", "", "")
	s.writeMetric(ctx, meta.UserID, "device_online", "device_online_count", 1, nil)
}

// LogDeviceOffline 设备离线日志（超时自动结算时记录）
func (s *LogService) LogDeviceOffline(ctx context.Context, meta EventMeta, deviceID string) {
	detail, _ := json.Marshal(map[string]string{"device_id": deviceID})
	s.writeLog(ctx, "device_offline", meta, string(detail), "", "", "", "")
	s.writeMetric(ctx, meta.UserID, "device_offline", "device_offline_count", 1, nil)
}

// LogTaskComplete 任务完成日志
func (s *LogService) LogTaskComplete(ctx context.Context, meta EventMeta, taskID int64, taskName string, rewardPoints int64) {
	detail, _ := json.Marshal(map[string]interface{}{
		"task_id":       taskID,
		"task_name":     taskName,
		"reward_points": rewardPoints,
	})
	s.writeLog(ctx, "task_complete", meta, string(detail), "", "", "", "")
	s.writeMetric(ctx, meta.UserID, "task_complete", "task_complete_count", 1, map[string]string{"task_name": taskName})

	// 额外记录奖励积分总值
	s.writeMetric(ctx, meta.UserID, "task_complete", "task_reward_points", float64(rewardPoints), nil)
}

// LogShopRedeem 商城兑换日志
func (s *LogService) LogShopRedeem(ctx context.Context, meta EventMeta, itemID int64, itemName string, pointsSpent int64) {
	detail, _ := json.Marshal(map[string]interface{}{
		"item_id":      itemID,
		"item_name":    itemName,
		"points_spent": pointsSpent,
	})
	s.writeLog(ctx, "shop_redeem", meta, string(detail), "", "", "", "")
	s.writeMetric(ctx, meta.UserID, "shop_redeem", "shop_redeem_count", 1, map[string]string{"item_name": itemName})

	// 额外记录消耗积分总值
	s.writeMetric(ctx, meta.UserID, "shop_redeem", "shop_redeem_points", float64(pointsSpent), nil)
}

// writeLog 非阻塞入队审计日志（缓冲满则丢弃并计数，绝不阻塞主链路）。
// loginType / loginResult / failReason / deviceInfo 为登录事件（event_type=login）专属字段，
// 其余事件传空字符串即可。
func (s *LogService) writeLog(ctx context.Context, eventType string, meta EventMeta, detail, loginType, loginResult, failReason, deviceInfo string) {
	if s.logRepo == nil {
		return // logRepo 为 nil 时静默跳过（与 flushLogs 守卫一致：对应协程 no-op）
	}
	entry := &model.AuditLog{
		ID:          nextID(),
		UserID:      meta.UserID,
		EventType:   eventType,
		LoginType:   loginType,
		LoginResult: loginResult,
		FailReason:  failReason,
		DeviceInfo:  deviceInfo,
		Detail:      detail,
		IPAddress:   meta.IPAddress,
		UserAgent:   meta.UserAgent,
		CreatedAt:   time.Now(),
	}

	select {
	case s.logCh <- entry:
	default:
		if n := s.droppedLogs.Add(1); n%1000 == 1 {
			log.Printf("[LogService] audit log buffer full, dropped %d entries so far", n)
		}
	}
}

// writeMetric 非阻塞入队监控指标（缓冲满则丢弃并计数，绝不阻塞主链路）。
func (s *LogService) writeMetric(ctx context.Context, userID, metricType, metricName string, value float64, tags map[string]string) {
	if s.monitorRepo == nil {
		return // monitorRepo 为 nil 时静默跳过（与 flushMetrics 守卫一致）
	}
	tagsJSON, _ := json.Marshal(tags)
	metric := &model.MonitorMetric{
		ID:         nextID(),
		UserID:     userID,
		MetricType: metricType,
		MetricName: metricName,
		Value:      value,
		Tags:       string(tagsJSON),
		CreatedAt:  time.Now(),
	}

	select {
	case s.metricCh <- metric:
	default:
		if n := s.droppedMetrics.Add(1); n%1000 == 1 {
			log.Printf("[LogService] monitor metric buffer full, dropped %d entries so far", n)
		}
	}
}

// logLoop 后台协程：攒批（达 flushBatchSize 或每 flushInterval）批量写审计日志。
func (s *LogService) logLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	batch := make([]*model.AuditLog, 0, flushBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		s.flushLogs(batch)
		batch = batch[:0]
	}

	for {
		select {
		case e := <-s.logCh:
			batch = append(batch, e)
			if len(batch) >= flushBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-s.stop:
			// drain 剩余队列后刷新退出
			for {
				select {
				case e := <-s.logCh:
					batch = append(batch, e)
					if len(batch) >= flushBatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// metricLoop 后台协程：攒批批量写监控指标。
func (s *LogService) metricLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	batch := make([]*model.MonitorMetric, 0, flushBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		s.flushMetrics(batch)
		batch = batch[:0]
	}

	for {
		select {
		case m := <-s.metricCh:
			batch = append(batch, m)
			if len(batch) >= flushBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-s.stop:
			for {
				select {
				case m := <-s.metricCh:
					batch = append(batch, m)
					if len(batch) >= flushBatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// flushLogs 批量写审计日志：优先走 BatchLogRepo，未实现则回退逐条写。
func (s *LogService) flushLogs(batch []*model.AuditLog) {
	if s.logRepo == nil {
		return // logRepo 为 nil 时静默丢弃（设计契约：任一 repo 为 nil 对应协程直接 no-op）
	}
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()

	if bw, ok := s.logRepo.(repository.BatchLogRepo); ok {
		if err := bw.CreateBatch(ctx, batch); err != nil {
			log.Printf("[LogService] batch write audit log failed (n=%d): %v", len(batch), err)
		}
		return
	}
	for _, e := range batch {
		if err := s.logRepo.Create(ctx, e); err != nil {
			log.Printf("[LogService] write audit log failed: %v", err)
		}
	}
}

// flushMetrics 批量写监控指标：优先走 BatchMonitorRepo，未实现则回退逐条写。
func (s *LogService) flushMetrics(batch []*model.MonitorMetric) {
	if s.monitorRepo == nil {
		return // monitorRepo 为 nil 时静默丢弃（设计契约：任一 repo 为 nil 对应协程直接 no-op）
	}
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()

	if bw, ok := s.monitorRepo.(repository.BatchMonitorRepo); ok {
		if err := bw.RecordBatch(ctx, batch); err != nil {
			log.Printf("[LogService] batch write monitor metric failed (n=%d): %v", len(batch), err)
		}
		return
	}
	for _, m := range batch {
		if err := s.monitorRepo.Record(ctx, m); err != nil {
			log.Printf("[LogService] write monitor metric failed: %v", err)
		}
	}
}

// BuildMeta 从 context 中提取用户信息构建 EventMeta
func BuildMeta(ctx context.Context, userID string) EventMeta {
	ip, _ := ctx.Value(contextkeys.ClientIP).(string)
	ua, _ := ctx.Value(contextkeys.UserAgent).(string)
	return EventMeta{
		UserID:    userID,
		IPAddress: ip,
		UserAgent: ua,
	}
}

// BuildMetaFromRequest 从 gin.Context 相关信息构建 EventMeta
func BuildMetaFromRequest(userID, ip, userAgent string) EventMeta {
	return EventMeta{
		UserID:    userID,
		IPAddress: ip,
		UserAgent: userAgent,
	}
}

// CountByType 统计指定时间段内某类事件总数（log.db 查询）
func (s *LogService) CountByType(ctx context.Context, eventType string, start, end time.Time) (int64, error) {
	if s.logRepo == nil {
		return 0, nil
	}
	return s.logRepo.CountByType(ctx, eventType, start, end)
}

// CountLogins 统计 [start,end] 内指定结果（success/fail）的登录次数。
// 合并 login_records 后登录是 audit_logs 的一类事件，必须用 login_result 过滤，
// 否则 CountByType("login") 会把成功与失败登录一起计入（合并复盘指出的真实隐患）。
func (s *LogService) CountLogins(ctx context.Context, result string, start, end time.Time) (int64, error) {
	if s.logRepo == nil {
		return 0, nil
	}
	return s.logRepo.CountByTypeAndResult(ctx, "login", result, start, end)
}

// MonitorSumByType 监控指标求和（monitor.db 查询）
func (s *LogService) MonitorSumByType(ctx context.Context, metricType string, start, end time.Time) (float64, error) {
	if s.monitorRepo == nil {
		return 0, nil // monitorRepo 为 nil 时静默返回 0，与 Count* 守卫一致（任一 repo 为 nil 即 no-op）
	}
	return s.monitorRepo.SumByType(ctx, metricType, start, end)
}
