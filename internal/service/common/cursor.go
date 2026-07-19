package common

// DefaultPageSize 游标分页默认每页大小（原 shop 子包 defaultPageSize，上移并导出）。
const DefaultPageSize = 20

// CursorPage 游标分页通用辅助：调用方应传入 limit+1 条以判断 hasMore，返回截断结果与下一页游标。
// limit<=0 时回退到 DefaultPageSize，避免 rows[:0] 空页 + hasMore 死循环。
// 原位于 shop 子包，因 idle 也需使用而上移到 common 并导出。
func CursorPage[T any](rows []T, limit int, id func(T) int64) ([]T, int64, bool) {
	if limit <= 0 {
		limit = DefaultPageSize
	}
	if len(rows) <= limit {
		return rows, 0, false
	}
	return rows[:limit], id(rows[limit-1]), true
}
