package db

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// MongoDBConfig MongoDB 连接配置
type MongoDBConfig struct {
	DSN         string
	Database    string
	Username    string
	Password    string
	AuthSource  string
	MinPoolSize uint64
	MaxPoolSize uint64
}

// MongoDBAdapter MongoDB 数据库适配器
type MongoDBAdapter struct {
	mu       sync.RWMutex
	client   *mongo.Client
	database *mongo.Database
	cfg      MongoDBConfig
	name     string
}

// NewMongoDBAdapter 创建 MongoDB 适配器
func NewMongoDBAdapter(cfg MongoDBConfig) *MongoDBAdapter {
	return &MongoDBAdapter{
		cfg:  cfg,
		name: "mongodb",
	}
}

// Name 返回适配器名称
func (a *MongoDBAdapter) Name() string { return a.name }

// Connect 建立连接
func (a *MongoDBAdapter) Connect(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	clientOpts := options.Client().ApplyURI(a.cfg.DSN).
		SetConnectTimeout(10 * time.Second).
		SetServerSelectionTimeout(10 * time.Second).
		SetMinPoolSize(a.cfg.MinPoolSize).
		SetMaxPoolSize(a.cfg.MaxPoolSize)

	// 如果单独提供了认证信息，设置 Auth（含 AuthSource）
	if a.cfg.Username != "" {
		cred := options.Credential{
			Username:   a.cfg.Username,
			Password:   a.cfg.Password,
			AuthSource: a.cfg.AuthSource,
		}
		clientOpts.SetAuth(cred)
	}

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return fmt.Errorf("mongodb connect: %w", err)
	}

	// 验证连接
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("mongodb ping: %w", err)
	}

	a.client = client

	// 确定使用的数据库：优先用显式配置的 Database，否则从 DSN 路径中提取
	database := a.cfg.Database
	if database == "" {
		database = extractDBFromDSN(a.cfg.DSN)
	}
	if database == "" {
		return fmt.Errorf("mongodb: no database specified (set in URI path or database field)")
	}
	a.database = client.Database(database)
	return nil
}

// extractDBFromDSN 从 MongoDB DSN 中提取数据库名
// "mongodb://user:pass@host:port/mydb?options" → "mydb"
func extractDBFromDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return ""
	}
	return strings.TrimPrefix(u.Path, "/")
}

// Close 关闭连接
func (a *MongoDBAdapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.client != nil {
		return a.client.Disconnect(context.Background())
	}
	return nil
}

// Ping 健康检查
func (a *MongoDBAdapter) Ping(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.client == nil {
		return fmt.Errorf("mongodb not connected")
	}
	return a.client.Ping(ctx, readpref.Primary())
}

// Client 返回 MongoDB 客户端
func (a *MongoDBAdapter) Client() *mongo.Client {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.client
}

// Database 返回 MongoDB 数据库实例
func (a *MongoDBAdapter) Database() *mongo.Database {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.database
}

// Collection 返回指定集合
func (a *MongoDBAdapter) Collection(name string) *mongo.Collection {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.database.Collection(name)
}

// DriverName 返回驱动名称
func (a *MongoDBAdapter) DriverName() string { return "mongodb" }
