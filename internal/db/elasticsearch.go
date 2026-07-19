package db

import (
	"context"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

// ElasticsearchAdapter Elasticsearch 适配器
type ElasticsearchAdapter struct {
	cfg         ElasticsearchCfg
	client      *elasticsearch.Client
	initialized bool
}

// ElasticsearchCfg ES 连接配置
type ElasticsearchCfg struct {
	Addresses   []string
	Username    string
	Password    string
	IndexPrefix string
}

// NewElasticsearchAdapter 创建 ES 适配器
func NewElasticsearchAdapter(cfg ElasticsearchCfg) *ElasticsearchAdapter {
	return &ElasticsearchAdapter{
		cfg: cfg,
	}
}

// Name 返回适配器名称
func (a *ElasticsearchAdapter) Name() string { return "elasticsearch" }

// Connect 建立连接
func (a *ElasticsearchAdapter) Connect(ctx context.Context) error {
	esCfg := elasticsearch.Config{
		Addresses: a.cfg.Addresses,
	}
	if a.cfg.Username != "" {
		esCfg.Username = a.cfg.Username
		esCfg.Password = a.cfg.Password
	}

	client, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return fmt.Errorf("elasticsearch new client: %w", err)
	}

	// 验证连接
	res, err := client.Ping(client.Ping.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("elasticsearch ping: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch ping error: %s", res.String())
	}

	a.client = client
	a.initialized = true
	return nil
}

// Close 关闭连接，释放底层 transport（http 连接池）。
// 幂等：未连接（client 为 nil）时直接返回 nil；重复调用亦安全。
func (a *ElasticsearchAdapter) Close() error {
	if a.client == nil {
		return nil
	}
	// elasticsearch.Client.Close 会关闭底层 elastictransport（默认 http.Transport），
	// 释放空闲连接；内部以 CAS 保证只会真正关闭一次。
	if err := a.client.Close(context.Background()); err != nil {
		return err
	}
	a.client = nil
	a.initialized = false
	return nil
}

// Ping 健康检查
func (a *ElasticsearchAdapter) Ping(ctx context.Context) error {
	if a.client == nil {
		return fmt.Errorf("elasticsearch not connected")
	}
	res, err := a.client.Ping(a.client.Ping.WithContext(ctx))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("elasticsearch ping error: %s", res.String())
	}
	return nil
}

// DB 返回原生连接
func (a *ElasticsearchAdapter) DB() interface{} {
	return a.client
}

// DriverName 返回驱动名称
func (a *ElasticsearchAdapter) DriverName() string {
	return "elasticsearch"
}

// Client 返回 ES 客户端
func (a *ElasticsearchAdapter) Client() *elasticsearch.Client {
	return a.client
}

// IndexPrefix 返回索引前缀
func (a *ElasticsearchAdapter) IndexPrefix() string {
	return a.cfg.IndexPrefix
}

// IndexName 返回带日期后缀的索引名（如 hc_logs-2026.07.08）
func (a *ElasticsearchAdapter) IndexName(ts time.Time) string {
	return fmt.Sprintf("%s-%s", a.cfg.IndexPrefix, ts.Format("2006.01.02"))
}
