package repository

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/pkg/logger"
)

// ESLoginRepository Elasticsearch 结构化登录记录仓库（需求 §6.9）。
// 与 ESLogRepository 同源：当 log.driver=elasticsearch 时，登录记录与审计日志一起写入 ES，
// 复用同一 ElasticsearchAdapter，避免重复建连。ES 不可用时由 bootstrap 统一回退 SQLite。
// 仅实现写路径（Create / CreateBatch / Close）；读操作（审计/风控查询）走 SQLite 落库。
type ESLoginRepository struct {
	adapter *db.ElasticsearchAdapter
}

// NewESLoginRepository 创建 ES 登录记录仓库
func NewESLoginRepository(adapter *db.ElasticsearchAdapter) *ESLoginRepository {
	return &ESLoginRepository{adapter: adapter}
}

// Close 关闭 Elasticsearch 连接（转发到 adapter；adapter.Close 释放底层 transport 连接池）。
func (r *ESLoginRepository) Close() error {
	return r.adapter.Close()
}

// SQLDB Elasticsearch 实现无 *sql.DB，返回错误（监控跳过该库连接池采集）。
func (r *ESLoginRepository) SQLDB() (*sql.DB, error) {
	return nil, errors.New("elasticsearch login repository has no *sql.DB")
}

// Create 写入一条结构化登录记录到 ES
func (r *ESLoginRepository) Create(ctx context.Context, rec *model.LoginRecord) error {
	// adapter 未成功 Connect 时 Client() 为 nil，直接调用会 panic；此处显式防御（13 §3.40）。
	if r.adapter == nil || r.adapter.Client() == nil {
		return fmt.Errorf("es login: client not connected")
	}
	if rec == nil {
		return fmt.Errorf("es login: nil LoginRecord passed to Create")
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("es login marshal: %w", err)
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
		return fmt.Errorf("es login index: %w", err)
	}
	if res.IsError() {
		return fmt.Errorf("es login index error: %s", res.String())
	}
	return nil
}

// CreateBatch 使用 ES Bulk API 批量写入结构化登录记录，显著降低网络往返与索引压力。
func (r *ESLoginRepository) CreateBatch(ctx context.Context, recs []*model.LoginRecord) error {
	if len(recs) == 0 {
		return nil
	}
	if r.adapter == nil || r.adapter.Client() == nil {
		return fmt.Errorf("es login: client not connected")
	}
	// 过滤 nil 元素，避免对 nil 元素 marshal 出 "null" 污染 bulk 请求（13 §3.40 ①）。
	clean := make([]*model.LoginRecord, 0, len(recs))
	for _, rec := range recs {
		if rec != nil {
			clean = append(clean, rec)
		}
	}
	if len(clean) == 0 {
		return nil
	}
	indexName := r.adapter.IndexName(time.Now())

	// 逐条序列化：单条 marshal 失败只跳过该条，不拖垮整批（13 §3.42 ③）。
	var buf bytes.Buffer
	skipped := 0
	for _, rec := range clean {
		body, err := json.Marshal(rec)
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
		return fmt.Errorf("es login bulk: all %d records failed to marshal", skipped)
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
		return fmt.Errorf("es login bulk: %w", err)
	}
	// 传输层错误（HTTP >= 300，如集群不可用 / 索引被封）：直接返回（13 §3.40）。
	if res.IsError() {
		return fmt.Errorf("es login bulk transport error: %s", res.String())
	}
	// HTTP 200 但部分文档写入失败时，res.IsError() 为 false，逐条失败会被静默吞掉；
	// 由 aggregateBulkErrors 解析 bulk 响应聚合逐条错误（13 §3.42 ①）。
	if err := aggregateBulkErrors(res.Body); err != nil {
		return err
	}
	// 存在因 marshal 失败被跳过的条目：批量整体成功，但记录上下文（非致命，不返回 error）。
	if skipped > 0 {
		logger.L().Warn(fmt.Sprintf("es login bulk: %d records skipped (marshal failed)", skipped))
	}
	return nil
}
