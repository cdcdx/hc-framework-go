package repository

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/pkg/logger"
)

// ESLogRepository Elasticsearch 日志仓库
type ESLogRepository struct {
	adapter *db.ElasticsearchAdapter
}

// NewESLogRepository 创建 ES 日志仓库
func NewESLogRepository(adapter *db.ElasticsearchAdapter) *ESLogRepository {
	return &ESLogRepository{adapter: adapter}
}

// Close 关闭 Elasticsearch 连接（转发到 adapter；adapter.Close 释放底层 transport 连接池）。
func (r *ESLogRepository) Close() error {
	if r.adapter == nil {
		return nil // 与读/写路径 nil-adapter 守卫一致，避免 shutdown 时 panic
	}
	return r.adapter.Close()
}

// SQLDB Elasticsearch 实现无 *sql.DB，返回错误（监控跳过该库连接池采集）。
func (r *ESLogRepository) SQLDB() (*sql.DB, error) {
	return nil, errors.New("elasticsearch repository has no *sql.DB")
}

// Create 写入一条审计日志到 ES
func (r *ESLogRepository) Create(ctx context.Context, entry *model.AuditLog) error {
	// adapter 未成功 Connect 时 Client() 为 nil，直接调用会 panic；此处显式防御（13 §3.40）。
	if r.adapter == nil || r.adapter.Client() == nil {
		return fmt.Errorf("es log: client not connected")
	}
	body, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("es log marshal: %w", err)
	}

	indexName := r.adapter.IndexName(time.Now())
	res, err := r.adapter.Client().Index(
		indexName,
		bytes.NewReader(body),
		r.adapter.Client().Index.WithContext(ctx),
	)
	// 无论 Index 是否返回 err，只要 res 非 nil 都必须关闭 Body，否则响应体泄漏（13 §3.40）。
	if res != nil {
		defer res.Body.Close()
	}
	if err != nil {
		return fmt.Errorf("es log index: %w", err)
	}

	if res.IsError() {
		return fmt.Errorf("es log index error: %s", res.String())
	}
	return nil
}

// CreateBatch 使用 ES Bulk API 批量写入审计日志，显著降低网络往返与索引压力。
func (r *ESLogRepository) CreateBatch(ctx context.Context, entries []*model.AuditLog) error {
	if len(entries) == 0 {
		return nil
	}
	if r.adapter == nil || r.adapter.Client() == nil {
		return fmt.Errorf("es log: client not connected")
	}
	indexName := r.adapter.IndexName(time.Now())

	// 逐条序列化：单条 marshal 失败只跳过该条，不拖垮整批（13 §3.42 ③）。
	var buf bytes.Buffer
	skipped := 0
	for _, e := range entries {
		body, err := json.Marshal(e)
		if err != nil {
			skipped++
			continue
		}
		buf.WriteString(`{"index":{}}`)
		buf.WriteByte('\n')
		buf.Write(body)
		buf.WriteByte('\n')
	}
	if buf.Len() == 0 {
		return fmt.Errorf("es log bulk: all %d entries failed to marshal", skipped)
	}

	res, err := r.adapter.Client().Bulk(
		bytes.NewReader(buf.Bytes()),
		r.adapter.Client().Bulk.WithContext(ctx),
		r.adapter.Client().Bulk.WithIndex(indexName),
	)
	if res != nil {
		defer res.Body.Close()
	}
	if err != nil {
		return fmt.Errorf("es log bulk: %w", err)
	}
	// 传输层错误（HTTP >= 300，如集群不可用 / 索引被封）：直接返回（13 §3.40）。
	if res.IsError() {
		return fmt.Errorf("es log bulk transport error: %s", res.String())
	}
	// HTTP 200 但部分文档写入失败（mapping 冲突 / 版本冲突等）时，res.IsError() 为 false，
	// 逐条失败会被静默吞掉；由 aggregateBulkErrors 解析 bulk 响应聚合逐条错误（13 §3.42 ①）。
	if err := aggregateBulkErrors(res.Body); err != nil {
		return err
	}
	// 存在因 marshal 失败被跳过的条目：批量整体成功，但记录上下文（非致命，不返回 error）。
	if skipped > 0 {
		logger.L().Warn(fmt.Sprintf("es log bulk: %d entries skipped (marshal failed)", skipped))
	}
	return nil
}

// aggregateBulkErrors 解析 ES Bulk 响应体：当 HTTP 200 但部分文档写入失败
// （errors=true）时，res.IsError() 仅判定传输层、会忽略逐条错误，此处将其聚合成单个 error 返回
// （13 §3.42 ①）。body 为 nil 或非法 JSON 时返回 decode 错误。
func aggregateBulkErrors(body io.Reader) error {
	var bulkRes struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Status int `json:"status"`
			Error  *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}
	if err := json.NewDecoder(body).Decode(&bulkRes); err != nil {
		return fmt.Errorf("es log bulk decode response: %w", err)
	}
	if !bulkRes.Errors {
		return nil
	}
	var fails []string
	for _, it := range bulkRes.Items {
		for action, item := range it {
			if item.Error != nil {
				fails = append(fails, fmt.Sprintf("%s:%d %s/%s", action, item.Status, item.Error.Type, item.Error.Reason))
			}
		}
	}
	return fmt.Errorf("es log bulk partial failures (%d/%d): %s", len(fails), len(bulkRes.Items), strings.Join(fails, "; "))
}

// FindByUser ES 暂不支持，返回空（读操作走监控面板或 SQLite）
func (r *ESLogRepository) FindByUser(ctx context.Context, userID string, cursor int64, limit int) ([]model.AuditLog, error) {
	return nil, fmt.Errorf("FindByUser not supported on Elasticsearch, use SQLite driver for queries")
}

// FindByType ES 暂不支持
func (r *ESLogRepository) FindByType(ctx context.Context, eventType string, start, end time.Time, limit int) ([]model.AuditLog, error) {
	return nil, fmt.Errorf("FindByType not supported on Elasticsearch, use SQLite driver for queries")
}

// CountByType ES 暂不支持
func (r *ESLogRepository) CountByType(ctx context.Context, eventType string, start, end time.Time) (int64, error) {
	return 0, fmt.Errorf("CountByType not supported on Elasticsearch, use SQLite driver for queries")
}

// CountByTypeAndResult ES 暂不支持
func (r *ESLogRepository) CountByTypeAndResult(ctx context.Context, eventType, result string, start, end time.Time) (int64, error) {
	return 0, fmt.Errorf("CountByTypeAndResult not supported on Elasticsearch, use SQLite driver for queries")
}
