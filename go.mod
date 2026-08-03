module github.com/cdcdx/hc-framework-go

go 1.25.0

// go-zero 迁移分支：gin 单体重构为 go-zero 微服务（rest 网关 + 领域 rpc）。
// 依赖版本由 goctl 生成 + `go mod tidy` 收敛；此处仅声明直接依赖。
require (
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/google/uuid v1.6.0
	github.com/zeromicro/go-zero v1.7.5
	golang.org/x/crypto v0.53.0
	google.golang.org/grpc v1.65.0
	google.golang.org/protobuf v1.36.8
	gorm.io/driver/mysql v1.6.0
	gorm.io/driver/postgres v1.6.0
	gorm.io/driver/sqlite v1.6.0
	gorm.io/gorm v1.31.2
)
