// Package degrade 提供全局「熔断触发降级」状态。
//
// 需求 §11：熔断器（circuit_breaker）触发打开时，强制全量降级为异步
// （读跳过 L2 直读 DB、写同步转异步 Kafka）。各组件（缓存策略引擎等）通过
// IsActive() 读取该状态，与配置项 cache.degrade.* 取「或」关系。
package degrade

import (
	"sync/atomic"
)

var (
	// openCount 当前处于 OPEN 状态的熔断器数量（按路由分组，互不影响）
	openCount int32
	// active 全局降级是否激活（openCount>0 时为 1）
	active int32
)

// OnBreakerOpen 熔断器打开时调用：打开计数 +1，首次打开时激活全局降级。
func OnBreakerOpen() {
	if atomic.AddInt32(&openCount, 1) == 1 {
		atomic.StoreInt32(&active, 1)
	}
}

// OnBreakerClosed 熔断器恢复（CLOSED）时调用：打开计数 -1，全部恢复时清除全局降级。
func OnBreakerClosed() {
	if atomic.AddInt32(&openCount, -1) == 0 {
		atomic.StoreInt32(&active, 0)
	}
}

// IsActive 返回当前是否处于熔断触发的全局降级状态。
func IsActive() bool {
	return atomic.LoadInt32(&active) == 1
}
