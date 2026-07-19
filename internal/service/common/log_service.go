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
	loginBufferSize  = 8192            // 登录记录缓冲队列容量
	metricBufferSize = 8192            // 监控指标缓冲队列容量
	flushBatchSize   = 500             // 达到该条数立即刷新
	flushInterval    = 1 * time.Second // 未达批量阈值时的最大驻留时长
	flushTimeout     = 10 * time.Second
)

// LogService 统一日志/监控服务（异步有界缓冲 + 批量刷新）
type LogService struct {
	logRepo     repository.LogRepo
	monitorRepo repository.MonitorRepo
	loginRepo   repository.LoginRepo // 结构化登录记录（需求 §6.9），可为 nil（不可用时 no-op）

	logCh    chan *model.AuditLog
	metricCh chan *model.MonitorMetric
	loginCh  chan *model.LoginRecord

	droppedLogs    atomic.Int64 // 因缓冲满被丢弃的审计日志数
	droppedMetrics atomic.Int64 // 因缓冲满被丢弃的监控指标数
	droppedLogin   atomic.Int64 // 因缓冲满被丢弃的登录记录数

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
func NewLogService(logRepo repository.LogRepo, monitorRepo repository.MonitorRepo, loginRepo repository.LoginRepo) *LogService {
	s := &LogService{
		logRepo:     logRepo,
		monitorRepo: monitorRepo,
		loginRepo:   loginRepo,
		logCh:       make(chan *model.AuditLog, logBufferSize),
		metricCh:    make(chan *model.MonitorMetric, metricBufferSize),
		loginCh:     make(chan *model.LoginRecord, loginBufferSize),
		stop:        make(chan struct{}),
	}
	s.wg.Add(3)
	go s.logLoop()
	go s.metricLoop()
	go s.loginLoop()
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
	s.writeLog(ctx, "register", meta, string(detail))
	s.writeMetric(ctx, meta.UserID, "register", "register_count", 1, nil)
}

// LogLogin 登录日志
func (s *LogService) LogLogin(ctx context.Context, meta EventMeta, email string, success bool) {
	detail, _ := json.Marshal(map[string]interface{}{
		"email":   email,
		"success": success,
	})
	s.writeLog(ctx, "login", meta, string(detail))
	if success {
		s.writeMetric(ctx, meta.UserID, "login", "login_count", 1, nil)
	}
}

// LogDeviceOnline 设备上线日志
func (s *LogService) LogDeviceOnline(ctx context.Context, meta EventMeta, deviceID string) {
	detail, _ := json.Marshal(map[string]string{"device_id": deviceID})
	s.writeLog(ctx, "device_online", meta, string(detail))
	s.writeMetric(ctx, meta.UserID, "device_online", "device_online_count", 1, nil)
}

// LogDeviceOffline 设备离线日志（超时自动结算时记录）
func (s *LogService) LogDeviceOffline(ctx context.Context, meta EventMeta, deviceID string) {
	detail, _ := json.Marshal(map[string]string{"device_id": deviceID})
	s.writeLog(ctx, "device_offline", meta, string(detail))
	s.writeMetric(ctx, meta.UserID, "device_offline", "device_offline_count", 1, nil)
}

// LogTaskComplete 任务完成日志
func (s *LogService) LogTaskComplete(ctx context.Context, meta EventMeta, taskID int64, taskName string, rewardPoints int64) {
	detail, _ := json.Marshal(map[string]interface{}{
		"task_id":       taskID,
		"task_name":     taskName,
		"reward_points": rewardPoints,
	})
	s.writeLog(ctx, "task_complete", meta, string(detail))
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
	s.writeLog(ctx, "shop_redeem", meta, string(detail))
	s.writeMetric(ctx, meta.UserID, "shop_redeem", "shop_redeem_count", 1, map[string]string{"item_name": itemName})

	// 额外记录消耗积分总值
	s.writeMetric(ctx, meta.UserID, "shop_redeem", "shop_redeem_points", float64(pointsSpent), nil)
}

// writeLog 非阻塞入队审计日志（缓冲满则丢弃并计数，绝不阻塞主链路）。
func (s *LogService) writeLog(ctx context.Context, eventType string, meta EventMeta, detail string) {
	entry := &model.AuditLog{
		ID:        nextID(),
		UserID:    meta.UserID,
		EventType: eventType,
		Detail:    detail,
		IPAddress: meta.IPAddress,
		UserAgent: meta.UserAgent,
		CreatedAt: time.Now(),
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

// RecordLogin 记录一条结构化登录记录（需求 §6.9）。
// 非阻塞入队（缓冲满则丢弃并计数），绝不阻塞主链路；loginRepo 为 nil 时直接 no-op。
func (s *LogService) RecordLogin(ctx context.Context, meta EventMeta, loginType, result, failReason, deviceInfo string) {
	if s == nil || s.loginRepo == nil {
		return
	}
	rec := &model.LoginRecord{
		UserID:      meta.UserID,
		LoginType:   loginType,
		IPAddress:   meta.IPAddress,
		DeviceInfo:  deviceInfo,
		LoginResult: result,
		FailReason:  failReason,
		CreatedAt:   time.Now(),
	}
	select {
	case s.loginCh <- rec:
	default:
		if n := s.droppedLogin.Add(1); n%1000 == 1 {
			log.Printf("[LogService] login record buffer full, dropped %d entries so far", n)
		}
	}
}

// loginLoop 后台协程：攒批（达 flushBatchSize 或每 flushInterval）批量写登录记录。
func (s *LogService) loginLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	batch := make([]*model.LoginRecord, 0, flushBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		s.flushLoginRecords(batch)
		batch = batch[:0]
	}

	for {
		select {
		case e := <-s.loginCh:
			batch = append(batch, e)
			if len(batch) >= flushBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-s.stop:
			for {
				select {
				case e := <-s.loginCh:
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

// flushLoginRecords 批量写登录记录（GORM CreateInBatches）。
func (s *LogService) flushLoginRecords(batch []*model.LoginRecord) {
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()
	if err := s.loginRepo.CreateBatch(ctx, batch); err != nil {
		log.Printf("[LogService] batch write login records failed (n=%d): %v", len(batch), err)
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
	return s.logRepo.CountByType(ctx, eventType, start, end)
}

// MonitorSumByType 监控指标求和（monitor.db 查询）
func (s *LogService) MonitorSumByType(ctx context.Context, metricType string, start, end time.Time) (float64, error) {
	return s.monitorRepo.SumByType(ctx, metricType, start, end)
}
