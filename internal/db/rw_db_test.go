package db

import (
	"context"
	"fmt"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/cdcdx/hc-framework-go/internal/model"
)

func openNamedDB(t *testing.T, name string) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", name)
	gdb, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open sqlite %s: %v", name, err)
	}
	if sqlDB, err := gdb.DB(); err == nil {
		sqlDB.SetMaxOpenConns(1)
		sqlDB.SetMaxIdleConns(1)
	}
	return gdb
}

// TestRWDB_ReadRoutesToSlave 验证有从库时默认读走从库，UseMaster 强制读主库。
func TestRWDB_ReadRoutesToSlave(t *testing.T) {
	master := openNamedDB(t, "rw_master")
	slave := openNamedDB(t, "rw_slave")
	master.AutoMigrate(&model.User{})
	slave.AutoMigrate(&model.User{})

	master.Create(&model.User{UserID: "u_m", Username: "master"})
	slave.Create(&model.User{UserID: "u_s", Username: "slave"})

	rw := NewRWDB(master, []*gorm.DB{slave})

	// 默认读走从库：能查到 slave 中的 u_s。
	got := &model.User{}
	if err := rw.Read(context.Background()).Where("user_id = ?", "u_s").First(got).Error; err != nil {
		t.Fatalf("Read should route to slave (u_s): %v", err)
	}

	// 强制主库：应查到 master 中的 u_m，而非 slave 的 u_s。
	gotM := &model.User{}
	if err := rw.Read(UseMaster(context.Background())).Where("user_id = ?", "u_m").First(gotM).Error; err != nil {
		t.Fatalf("Read(UseMaster) should route to master (u_m): %v", err)
	}
}

// TestRWDB_WriteAndTransactionUseMaster 验证写与事务始终走主库。
func TestRWDB_WriteAndTransactionUseMaster(t *testing.T) {
	master := openNamedDB(t, "rw_master_w")
	slave := openNamedDB(t, "rw_slave_w")
	master.AutoMigrate(&model.User{})
	slave.AutoMigrate(&model.User{})

	rw := NewRWDB(master, []*gorm.DB{slave})

	if err := rw.Write(context.Background()).Create(&model.User{UserID: "u_w", Email: "w@example.com"}).Error; err != nil {
		t.Fatal(err)
	}
	var cm, cs int64
	master.Model(&model.User{}).Where("user_id = ?", "u_w").Count(&cm)
	slave.Model(&model.User{}).Where("user_id = ?", "u_w").Count(&cs)
	if cm != 1 {
		t.Fatalf("write should land on master, cm=%d", cm)
	}
	if cs != 0 {
		t.Fatalf("write should NOT land on slave, cs=%d", cs)
	}

	if err := rw.Transaction(context.Background(), func(tx *gorm.DB) error {
		return tx.Create(&model.User{UserID: "u_t", Email: "t@example.com"}).Error
	}); err != nil {
		t.Fatal(err)
	}
	var ct int64
	master.Model(&model.User{}).Where("user_id = ?", "u_t").Count(&ct)
	if ct != 1 {
		t.Fatalf("transaction should land on master, ct=%d", ct)
	}
}

// TestRWDB_NoSlaveFallsBackToMaster 验证无从库时读自动降级到主库。
func TestRWDB_NoSlaveFallsBackToMaster(t *testing.T) {
	master := openNamedDB(t, "rw_master_only")
	master.AutoMigrate(&model.User{})
	master.Create(&model.User{UserID: "u_m", Username: "master"})

	rw := NewRWDB(master, nil)

	got := &model.User{}
	if err := rw.Read(context.Background()).Where("user_id = ?", "u_m").First(got).Error; err != nil {
		t.Fatalf("Read with no slave should fall back to master: %v", err)
	}
}

// TestRWDB_Ping 验证主从健康探测对内存库返回成功。
func TestRWDB_Ping(t *testing.T) {
	master := openNamedDB(t, "rw_master_ping")
	slave := openNamedDB(t, "rw_slave_ping")
	rw := NewRWDB(master, []*gorm.DB{slave})
	if err := rw.Ping(context.Background()); err != nil {
		t.Fatalf("Ping should succeed for in-memory dbs: %v", err)
	}
}

// TestRWDB_SlaveHealth 验证从库初始健康状态标记为健康。
func TestRWDB_SlaveHealth(t *testing.T) {
	master := openNamedDB(t, "rw_master_h")
	slave := openNamedDB(t, "rw_slave_h")
	rw := NewRWDB(master, []*gorm.DB{slave})
	health := rw.SlaveHealth()
	if len(health) != 1 || !health[0] {
		t.Fatalf("SlaveHealth = %v, want {0:true}", health)
	}
}
