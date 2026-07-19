package common

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/model"
)

// fakeLoginRepo 内存实现，用于验证 RecordLogin 写入。
type fakeLoginRepo struct {
	mu   sync.Mutex
	recs []*model.LoginRecord
}

func (f *fakeLoginRepo) Create(_ context.Context, rec *model.LoginRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recs = append(f.recs, rec)
	return nil
}

func (f *fakeLoginRepo) CreateBatch(_ context.Context, recs []*model.LoginRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recs = append(f.recs, recs...)
	return nil
}
func (f *fakeLoginRepo) Close() error            { return nil }
func (f *fakeLoginRepo) SQLDB() (*sql.DB, error) { return nil, nil }

func (f *fakeLoginRepo) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.recs)
}

func TestLogService_RecordLogin(t *testing.T) {
	fake := &fakeLoginRepo{}
	s := &LogService{
		loginRepo: fake,
		loginCh:   make(chan *model.LoginRecord, 64),
		stop:      make(chan struct{}),
	}
	s.wg.Add(1)
	go s.loginLoop()

	s.RecordLogin(context.Background(), EventMeta{UserID: "u1", IPAddress: "1.2.3.4"}, model.LoginTypePassword, model.LoginResultSuccess, "", "iPhone")

	// Close 触发 loginLoop 排空并 flush 剩余缓冲（确定性，无需等待 ticker）。
	s.Close()

	if got := fake.count(); got != 1 {
		t.Fatalf("login records written = %d, want 1", got)
	}
	if got := fake.recs[0].LoginType; got != model.LoginTypePassword {
		t.Fatalf("login_type = %q, want %q", got, model.LoginTypePassword)
	}
	if got := fake.recs[0].LoginResult; got != model.LoginResultSuccess {
		t.Fatalf("login_result = %q, want %q", got, model.LoginResultSuccess)
	}
}

func TestLogService_RecordLogin_NilRepoNoop(t *testing.T) {
	// loginRepo 为 nil 时 RecordLogin 必须安全返回，不 panic、不写入。
	s := &LogService{}
	s.RecordLogin(context.Background(), EventMeta{UserID: "u1"}, model.LoginTypeGoogle, model.LoginResultFail, "oauth_failed", "")
	// 无 repo 则无写；若走到此处未 panic 即通过。
	time.Sleep(10 * time.Millisecond)
}
