package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/cdcdx/hc-framework-go/internal/db"
	"github.com/cdcdx/hc-framework-go/internal/model"
)

const usersCollection = "users"

// mongoUserRepository MongoDB 用户数据仓库
type mongoUserRepository struct {
	adapter *db.MongoDBAdapter
}

// NewMongoUserRepository 创建 MongoDB 用户仓库
func NewMongoUserRepository(adapter *db.MongoDBAdapter) UserRepository {
	return &mongoUserRepository{adapter: adapter}
}

// AutoMigrate 确保索引（MongoDB 不需要建表，集合自动创建）
func (r *mongoUserRepository) AutoMigrate() error {
	ctx := context.Background()
	col := r.adapter.Collection(usersCollection)

	// user_id 唯一索引
	_, err := col.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "user_id", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("idx_user_id"),
	})
	if err != nil && !isIndexExists(err) {
		return fmt.Errorf("create user_id index: %w", err)
	}

	// email 唯一索引
	_, err = col.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "email", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("idx_email"),
	})
	if err != nil && !isIndexExists(err) {
		return fmt.Errorf("create email index: %w", err)
	}

	// google_id 唯一索引（稀疏索引，仅对有值的文档生效）
	_, err = col.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "google_id", Value: 1}},
		Options: options.Index().SetUnique(true).SetSparse(true).SetName("idx_google_id"),
	})
	if err != nil && !isIndexExists(err) {
		return fmt.Errorf("create google_id index: %w", err)
	}

	return nil
}

// isIndexExists 检查 MongoDB 错误是否为"索引已存在"
func isIndexExists(err error) bool {
	cmdErr, ok := err.(mongo.CommandError)
	return ok && cmdErr.Code == 85 // IndexAlreadyExists
}

// Create 创建用户
func (r *mongoUserRepository) Create(ctx context.Context, user *model.User) error {
	_, err := r.adapter.Collection(usersCollection).InsertOne(ctx, user)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("user already exists")
		}
		return err
	}
	return nil
}

// FindByID 根据 UserID 查询
func (r *mongoUserRepository) FindByID(ctx context.Context, userID string) (*model.User, error) {
	var user model.User
	err := r.adapter.Collection(usersCollection).FindOne(ctx, bson.M{"user_id": userID}).Decode(&user)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return &user, nil
}

// FindByEmail 根据邮箱查询
func (r *mongoUserRepository) FindByEmail(ctx context.Context, email string) (*model.User, error) {
	var user model.User
	err := r.adapter.Collection(usersCollection).FindOne(ctx, bson.M{"email": email}).Decode(&user)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return &user, nil
}

// FindByGoogleID 根据 Google ID 查询
func (r *mongoUserRepository) FindByGoogleID(ctx context.Context, googleID string) (*model.User, error) {
	var user model.User
	err := r.adapter.Collection(usersCollection).FindOne(ctx, bson.M{"google_id": googleID}).Decode(&user)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return &user, nil
}

// Update 更新用户
func (r *mongoUserRepository) Update(ctx context.Context, user *model.User) error {
	_, err := r.adapter.Collection(usersCollection).ReplaceOne(
		ctx,
		bson.M{"user_id": user.UserID},
		user,
	)
	return err
}

// UpdatePoints 原子更新积分余额
func (r *mongoUserRepository) UpdatePoints(ctx context.Context, userID string, amount int64) error {
	result, err := r.adapter.Collection(usersCollection).UpdateOne(
		ctx,
		bson.M{"user_id": userID, "points_balance": bson.M{"$gte": -amount}},
		bson.M{"$inc": bson.M{"points_balance": amount}},
	)
	if err != nil {
		return err
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("insufficient points or user not found")
	}
	return nil
}

// UpdatePassword 更新密码哈希和修改时间
func (r *mongoUserRepository) UpdatePassword(ctx context.Context, userID, passwordHash string, changedAt interface{}) error {
	_, err := r.adapter.Collection(usersCollection).UpdateOne(
		ctx,
		bson.M{"user_id": userID},
		bson.M{"$set": bson.M{
			"password_hash":       passwordHash,
			"password_changed_at": changedAt,
		}},
	)
	return err
}

// Close 断开 MongoDB client 连接，释放 userDB 连接池（优雅关闭时调用）。
func (r *mongoUserRepository) Close() error {
	return r.adapter.Close()
}

// SQLDB MongoDB 实现无 sql.DB，返回错误（监控跳过该库连接池采集）。
func (r *mongoUserRepository) SQLDB() (*sql.DB, error) {
	return nil, errors.New("mongodb repository has no *sql.DB")
}
