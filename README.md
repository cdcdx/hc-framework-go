# HC Framework (Go)

面向 C 端用户的**通用高并发后台框架**，承载用户注册登录、在线挂机、任务奖励、积分兑换等核心业务场景。

> 详细设计、部署与运维文档已拆分到 [000_文档导航.md](docs/000_文档导航.md)（按分类索引）。本文件为项目入口，仅保留速查与摘要，详情均链接到对应文档。

## 目录

- [快速开始](#快速开始)
- [实现状态](#实现状态)
- [项目结构](#项目结构)
- [核心能力概览](#核心能力概览)
- [文档索引](#文档索引)
- [License](#license)

## 快速开始

### 前置条件

- Go ≥ 1.25
- （可选）Redis 7.x / Valkey 8.x
- （可选）MySQL 8.x / PostgreSQL 16.x

### SQLite 模式（零依赖启动）

```bash
git clone <repo-url>
cd hc-framework-go
go mod download
make run
```

服务启动后访问：

- API 文档: `http://localhost:8080/swagger/index.html`
- 健康检查: `http://localhost:8080/health`
- 就绪检查: `http://localhost:8080/ready`

### 常用命令

> 数据库 / Docker / 压测等运维命令速查见 [404_运维命令手册.md](docs/404_运维命令手册.md)。

```bash
make build        # 编译
make run          # 运行
make test         # 测试
make lint         # 代码检查
make docker-build # Docker 构建
make clean        # 清理构建产物
```

## 实现状态

本框架接口与代码分层完整，但部分能力处于「接口已定义 / 部分实现」阶段。当前真实落地情况（已具备能力、已知限制、近期路线、测试覆盖）见 **[602_实现状态与路线图.md](docs/602_实现状态与路线图.md)**（与 [001_需求.md](docs/001_需求.md) 对照）。

- 已实现：三级缓存（L1+L2+策略引擎）、缓存防护、读写分离、JWT 鉴权与黑名单、四层过载防护（连接级上限 / 应用层有界并发·负载卸载 / 限流 / 熔断）、验证码防水墙、账号安全、配置热更新与读写降级、多数据库适配器、事件驱动任务进度、链路追踪、定时任务、流量降级、优雅退出排水。
- 已知限制与规划：热点 Key 本地预取（部分）、Kafka 消费者无消费组模式等，详见 [405_安全与防护.md](docs/405_安全与防护.md) 与 [603_代码优化记录.md](docs/603_代码优化记录.md)。（RabbitMQ / RocketMQ 适配器**已实现**，见 [602_实现状态与路线图.md](docs/602_实现状态与路线图.md)）

## 项目结构

> 完整文件树以本节为权威来源；文档中的「项目结构」章节均引用此处。

```
.
├── cmd/server/main.go          # 入口
├── internal/
│   ├── config/                 # 配置加载与热更新（Manager）
│   ├── bootstrap/              # 组合根：装配 DB/Service/路由/后台任务，封装启动与优雅关闭辅助
│   ├── middleware/              # 中间件链
│   ├── handler/                # HTTP 处理器
│   ├── service/                # 业务逻辑
│   ├── repository/             # 数据访问层
│   ├── model/                  # 数据模型
│   ├── router/                 # 路由注册
│   ├── db/                     # 数据库适配器
│   ├── cache/                  # 三级缓存（L1/L2 + 布隆 + 热点 Key）
│   ├── mq/                     # 消息队列（Kafka 生产者/消费者）
│   ├── degrade/                # 流量降级（读/写降级）
│   ├── event/                  # 事件消息结构
│   ├── metrics/                # Prometheus 指标采集
│   ├── scheduler/              # 定时任务（超时结算/周期重置）
│   ├── trace/                  # 链路追踪（OTel 兼容, W3C traceparent）
│   └── cluster/                # 选主（Redis 租约，多副本单例任务）
├── pkg/                        # 公共工具库（bcrypt/captcha/jwt/mail/logger/response/contextkeys/...）
├── docs/                       # 文档（000~800，按分类索引于 000_文档导航.md）
├── scripts/                    # 脚本（迁移 migrate.sh / 压测 k6·loadtest / 容量校验）
├── config/config.yaml          # 默认配置
├── config/config.example.yaml  # 完整配置示例（带注释）
├── swagger/                    # Swagger 文档（swag init 生成）
├── deployments/                # K8s 部署清单
├── migrations/                 # 数据库迁移（golang-migrate）
├── build.sh                    # 构建脚本
├── docker-compose.kafka.yml    # 本地 Kafka 一键编排
├── Dockerfile
├── Makefile
└── README.md
```

## 核心能力概览

请求按「全局链 → 路由组链 → 路由级链」分层经过中间件：`Recovery → TraceID → RequestInfo → RequestLogger → CORS → Metrics → ConcurrencyLimit(负载卸载) → RateLimit`；认证接口叠加 `SecurityIPLimit → Captcha`；业务路由叠加 `Auth → CircuitBreaker`。

三级防护覆盖「连接级快速失败（MaxConns）→ 应用层有界并发/负载卸载（503）→ 令牌桶限流（429）→ 熔断（下游降级）」四层过载保护，维度互补、不可互相替代（设计取舍见 [102_高并发架构设计实践.md](docs/102_高并发架构设计实践.md)）。就绪探针在优雅关闭时返回 503 触发 K8s 排水，实现「先摘流量、再排空中途请求」的零中断下线。

三级缓存：L1 Ristretto（<1ms）/ L2 Redis（1-5ms）/ L3 主存储（10-200ms）；穿透（布隆+空值）、击穿（SETNX 分布式锁）、雪崩（TTL 抖动）、一致性（延迟双删+广播失效）防护齐全。

> 完整架构图、组件级联、Pod Sharding、故障回退见 [101_架构说明.md](docs/101_架构说明.md)；指标/告警/故障排查见 [403_可观测性与告警.md](docs/403_可观测性与告警.md)；安全与防护见 [405_安全与防护.md](docs/405_安全与防护.md)。

## 文档索引

| 主题 | 文档 |
|------|------|
| 需求基线 / 详细规格 | [001_需求.md](docs/001_需求.md) · [002_需求规格说明书.md](docs/002_需求规格说明书.md) |
| 架构 | [101_架构说明.md](docs/101_架构说明.md) |
| 接口契约 / 错误码 | [201_API参考.md](docs/201_API参考.md) |
| 配置 / 迁移 | [202_配置参考.md](docs/202_配置参考.md) · [203_数据库迁移.md](docs/203_数据库迁移.md) |
| 部署 | [401_集群设备部署手册.md](docs/401_集群设备部署手册.md) · [402_Redis集群部署指南.md](docs/402_Redis集群部署指南.md) |
| 运维 / 安全 | [403_可观测性与告警.md](docs/403_可观测性与告警.md) · [404_运维命令手册.md](docs/404_运维命令手册.md) · [405_安全与防护.md](docs/405_安全与防护.md) |
| 进度 / 开发 | [602_实现状态与路线图.md](docs/602_实现状态与路线图.md) · [603_代码优化记录.md](docs/603_代码优化记录.md) · [601_测试与开发指南.md](docs/601_测试与开发指南.md) |
| 技术参考 | [501_数据库技术选型参考.md](docs/501_数据库技术选型参考.md) · [502_队列技术选型参考.md](docs/502_队列技术选型参考.md) |
| 启动与生命周期 | [504_启动流程与生命周期.md](docs/504_启动流程与生命周期.md) |
| 重构基线 / 优化清单 | [701_重构前基线梳理.md](docs/701_重构前基线梳理.md) · [702_优化清单与后续建议.md](docs/702_优化清单与后续建议.md) |
| 高并发设计 | [102_高并发架构设计实践.md](docs/102_高并发架构设计实践.md) · [302_定时抢购高并发方案.md](docs/302_定时抢购高并发方案.md) · [301_库存分桶设计.md](docs/301_库存分桶设计.md) |
| 架构专题 | [103_事件与消息总线.md](docs/103_事件与消息总线.md) · [104_链路追踪架构.md](docs/104_链路追踪架构.md) |
| 功能全景 | [800_功能全景.md](docs/800_功能全景.md) | 全量功能清单与代码规模统计 |

各主题的接口字段、配置字段、数据模型、错误码、部署清单、容量评估、开发规范等**完整细节均已下沉到上述文档**，本文不再重复。

## License

MIT
