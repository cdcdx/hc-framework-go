package config

import (
	"testing"
	"time"
)

func TestResolveDSN(t *testing.T) {
	tests := []struct {
		name       string
		cfg        DBConfig
		wantDriver string
		wantDSN    string
		wantErr    bool
	}{
		{
			name:       "sqlite 顶层 Dsn",
			cfg:        DBConfig{Driver: "sqlite", Dsn: "file:./hc.db"},
			wantDriver: "sqlite", wantDSN: "file:./hc.db",
		},
		{
			name:       "mysql vendor 段优先于顶层 Dsn",
			cfg:        DBConfig{Driver: "mysql", Dsn: "fallback", Mysql: DBVendorConfig{Master: "u:p@tcp(h:3306)/db"}},
			wantDriver: "mysql", wantDSN: "u:p@tcp(h:3306)/db",
		},
		{
			name:       "mysql vendor 缺失时回退顶层 Dsn",
			cfg:        DBConfig{Driver: "mysql", Dsn: "fallback"},
			wantDriver: "mysql", wantDSN: "fallback",
		},
		{
			name:       "driver=sqlite 不得误取 mysql 段",
			cfg:        DBConfig{Driver: "sqlite", Dsn: "file:./hc.db", Mysql: DBVendorConfig{Master: "should-not-be-used"}},
			wantDriver: "sqlite", wantDSN: "file:./hc.db",
		},
		{
			name:       "driver 为空时从 vendor 段推断",
			cfg:        DBConfig{Postgres: DBVendorConfig{Master: "postgres://h/db"}},
			wantDriver: "postgres", wantDSN: "postgres://h/db",
		},
		{
			name:    "无任何 DSN 应报错",
			cfg:     DBConfig{},
			wantErr: true,
		},
		{
			name:    "driver 指定但 DSN 为空应报错",
			cfg:     DBConfig{Driver: "mysql"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			driver, dsn, err := tc.cfg.ResolveDSN()
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if driver != tc.wantDriver || dsn != tc.wantDSN {
				t.Errorf("got (%q, %q), want (%q, %q)", driver, dsn, tc.wantDriver, tc.wantDSN)
			}
		})
	}
}

func TestResolvePoolVendorPriority(t *testing.T) {
	cfg := DBConfig{
		Driver:          "mysql",
		MaxOpenConns:    50,
		MaxIdleConns:    10,
		ConnMaxLifetime: time.Hour,
		Mysql: DBVendorConfig{
			Master: "dsn", MaxOpenConns: 200, MaxIdleConns: 40, ConnMaxLifetime: 30 * time.Minute,
		},
	}
	maxOpen, maxIdle, connMax := cfg.resolvePool("mysql")
	if maxOpen != 200 || maxIdle != 40 || connMax != 30*time.Minute {
		t.Errorf("vendor 段应覆盖顶层: got (%d, %d, %v)", maxOpen, maxIdle, connMax)
	}

	// vendor 未配置连接池时保留顶层值
	cfg2 := DBConfig{Driver: "mysql", MaxOpenConns: 50, MaxIdleConns: 10, ConnMaxLifetime: time.Hour}
	maxOpen2, maxIdle2, connMax2 := cfg2.resolvePool("mysql")
	if maxOpen2 != 50 || maxIdle2 != 10 || connMax2 != time.Hour {
		t.Errorf("应保留顶层值: got (%d, %d, %v)", maxOpen2, maxIdle2, connMax2)
	}

	// sqlite 无 vendor 段，取顶层值
	cfg3 := DBConfig{Driver: "sqlite", MaxOpenConns: 5}
	if maxOpen3, _, _ := cfg3.resolvePool("sqlite"); maxOpen3 != 5 {
		t.Errorf("sqlite 应取顶层值, got %d", maxOpen3)
	}

	// vendor 段各字段独立覆盖：MaxOpenConns>0 不应连带覆盖 MaxIdleConns。
	cfg4 := DBConfig{
		Driver:       "mysql",
		MaxOpenConns: 50,
		MaxIdleConns: 10,
		Mysql:        DBVendorConfig{Master: "dsn", MaxOpenConns: 200}, // 仅覆盖 open
	}
	maxOpen4, maxIdle4, _ := cfg4.resolvePool("mysql")
	if maxOpen4 != 200 {
		t.Errorf("vendor MaxOpenConns 应覆盖顶层: got %d", maxOpen4)
	}
	if maxIdle4 != 10 {
		t.Errorf("vendor 未设 MaxIdleConns 时应保留顶层值: got %d", maxIdle4)
	}
}

// TestToSpecs 验证 config → dbclient 的适配：DSN 解析与连接池优先级
// 均在此完成，dbclient 只消费结果。
func TestToSpecs(t *testing.T) {
	dbs := Databases{
		"business": {Driver: "sqlite", Dsn: "file::memory:", PoolLabel: "biz"},
		"user": {
			Driver: "mysql",
			Mysql:  DBVendorConfig{Master: "u:p@tcp(h:3306)/user", MaxOpenConns: 300, MaxIdleConns: 60},
		},
	}
	specs := dbs.ToSpecs()

	if len(specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(specs))
	}

	biz := specs["business"]
	if biz.Driver != "sqlite" || biz.DSN != "file::memory:" || biz.PoolLabel != "biz" {
		t.Errorf("business spec 不正确: %+v", biz)
	}
	if !biz.IsSQL() {
		t.Error("sqlite 应被识别为 SQL")
	}

	user := specs["user"]
	if user.Driver != "mysql" || user.DSN != "u:p@tcp(h:3306)/user" {
		t.Errorf("user DSN 解析错误: %+v", user)
	}
	if user.MaxOpenConns != 300 || user.MaxIdleConns != 60 {
		t.Errorf("vendor 段连接池未生效: %+v", user)
	}
}

// TestToSpecsSkipsUnresolvable 解析失败的库应保留 Driver 以便 Factory 报出有意义的错误，
// 且不能影响其他库的装配。
func TestToSpecsSkipsUnresolvable(t *testing.T) {
	dbs := Databases{
		"good": {Driver: "sqlite", Dsn: "file::memory:"},
		"bad":  {Driver: "mysql"}, // 无 DSN
	}
	specs := dbs.ToSpecs()

	if specs["good"].DSN == "" {
		t.Error("正常库不应受影响")
	}
	if specs["bad"].DSN != "" {
		t.Error("解析失败的库 DSN 应为空")
	}
	if specs["bad"].Driver != "mysql" {
		t.Error("解析失败的库应保留 Driver 以便报错")
	}
}

func TestDBConfigIsSQL(t *testing.T) {
	cases := map[string]struct {
		cfg   DBConfig
		isSQL bool
	}{
		"sqlite":        {DBConfig{Driver: "sqlite", Dsn: "file::memory:"}, true},
		"mysql":         {DBConfig{Driver: "mysql", Dsn: "dsn"}, true},
		"postgres":      {DBConfig{Driver: "postgres", Dsn: "dsn"}, true},
		"postgresql":    {DBConfig{Driver: "postgresql", Dsn: "dsn"}, true},
		"mongodb":       {DBConfig{Driver: "mongodb", Dsn: "dsn"}, false},
		"elasticsearch": {DBConfig{Driver: "elasticsearch", Dsn: "dsn"}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := tc.cfg.IsSQL(); got != tc.isSQL {
				t.Errorf("IsSQL() = %v, want %v", got, tc.isSQL)
			}
		})
	}
}
