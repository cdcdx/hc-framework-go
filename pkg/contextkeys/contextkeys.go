// Package contextkeys 上下文键（context key）定义。
package contextkeys

type contextKey string

const (
	ClientIP  contextKey = "client_ip"
	UserAgent contextKey = "user_agent"
)
