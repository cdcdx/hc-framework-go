// Package handler WebSocket 连接管理与消息推送。
//
// 设计目标：
//   - 全双工通信：替代 HTTP 心跳轮询，降低网络开销
//   - 实时推送：事件驱动通知（积分变动、任务进度、系统公告）
//   - 水平扩展：通过 UserID 索引支持多设备连接，跨 Pod 推送需配合 MQ 广播
//
// Message 协议（JSON）：
//
//	Client → Server:
//	  {"type":"ping"}                            — 心跳保活
//	  {"type":"subscribe","topics":["points"]}   — 订阅主题
//
//	Server → Client:
//	  {"type":"pong"}                            — 心跳响应
//	  {"type":"notification","topic":"points","data":{...}} — 通知推送
package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// ==========================================
// 常量
// ==========================================

const (
	writeWait      = 10 * time.Second  // 写超时
	pongWait       = 60 * time.Second  // Pong 等待超时
	pingPeriod     = (pongWait * 9) / 10 // Ping 发送周期
	maxMessageSize = 4096              // 最大消息大小
	sendBufSize    = 64                // 发送缓冲大小
)

// ==========================================
// 消息类型
// ==========================================

// wsMessage 客户端↔服务端的统一消息结构。
type wsMessage struct {
	Type   string          `json:"type"`
	Topics []string        `json:"topics,omitempty"`
	Topic  string          `json:"topic,omitempty"`
	Data   json.RawMessage `json:"data,omitempty"`
}

// ==========================================
// wsClient
// ==========================================

// wsClient 代表一个 WebSocket 连接。
type wsClient struct {
	hub    *WebSocketHub
	conn   *websocket.Conn
	userID string
	send   chan []byte
	topics map[string]struct{}

	mu sync.RWMutex
}

func newWSClient(hub *WebSocketHub, conn *websocket.Conn, userID string) *wsClient {
	return &wsClient{
		hub:    hub,
		conn:   conn,
		userID: userID,
		send:   make(chan []byte, sendBufSize),
		topics: make(map[string]struct{}),
	}
}

// readPump 读取客户端消息。
func (c *wsClient) readPump(logger *zap.Logger) {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, msgBytes, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				logger.Debug("ws read error", zap.String("user", c.userID), zap.Error(err))
			}
			break
		}

		var msg wsMessage
		if err := json.Unmarshal(msgBytes, &msg); err != nil {
			logger.Debug("ws invalid message", zap.String("user", c.userID), zap.Error(err))
			continue
		}

		switch msg.Type {
		case "ping":
			c.sendJSON(wsMessage{Type: "pong"})
		case "subscribe":
			c.mu.Lock()
			for _, topic := range msg.Topics {
				c.topics[topic] = struct{}{}
			}
			c.mu.Unlock()
			logger.Debug("ws subscribe", zap.String("user", c.userID), zap.Strings("topics", msg.Topics))
		default:
			logger.Debug("ws unknown type", zap.String("user", c.userID), zap.String("type", msg.Type))
		}
	}
}

// writePump 向客户端写入消息。
func (c *wsClient) writePump(logger *zap.Logger) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				logger.Debug("ws write error", zap.String("user", c.userID), zap.Error(err))
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// sendJSON 向客户端发送 JSON 消息（非阻塞）。
func (c *wsClient) sendJSON(msg wsMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	select {
	case c.send <- data:
	default:
	}
}

// ==========================================
// WebSocketHub
// ==========================================

// WebSocketHub 管理所有 WebSocket 连接。
type WebSocketHub struct {
	clients    map[string][]*wsClient
	register   chan *wsClient
	unregister chan *wsClient
	broadcast  chan []byte
	logger     *zap.Logger
	mu         sync.RWMutex
}

// NewWebSocketHub 创建 WebSocket Hub。
func NewWebSocketHub(logger *zap.Logger) *WebSocketHub {
	return &WebSocketHub{
		clients:    make(map[string][]*wsClient),
		register:   make(chan *wsClient, 256),
		unregister: make(chan *wsClient, 256),
		broadcast:  make(chan []byte, 256),
		logger:     logger,
	}
}

// Run 启动 Hub 事件循环（阻塞，应在独立 goroutine 中运行）。
func (h *WebSocketHub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			h.closeAll()
			return
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client.userID] = append(h.clients[client.userID], client)
			h.mu.Unlock()
		case client := <-h.unregister:
			h.mu.Lock()
			h.removeClient(client)
			h.mu.Unlock()
		case msg := <-h.broadcast:
			h.mu.RLock()
			for _, clients := range h.clients {
				for _, c := range clients {
					select {
					case c.send <- msg:
					default:
					}
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (h *WebSocketHub) removeClient(client *wsClient) {
	clients := h.clients[client.userID]
	for i, c := range clients {
		if c == client {
			h.clients[client.userID] = append(clients[:i], clients[i+1:]...)
			break
		}
	}
	if len(h.clients[client.userID]) == 0 {
		delete(h.clients, client.userID)
	}
	close(client.send)
}

func (h *WebSocketHub) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for userID, clients := range h.clients {
		for _, c := range clients {
			close(c.send)
		}
		delete(h.clients, userID)
	}
}

// SendToUser 向指定用户推送消息（多设备全推）。
func (h *WebSocketHub) SendToUser(userID string, msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, c := range h.clients[userID] {
		select {
		case c.send <- msg:
		default:
		}
	}
}

// Broadcast 向所有连接广播消息。
func (h *WebSocketHub) Broadcast(msg []byte) {
	select {
	case h.broadcast <- msg:
	default:
		h.logger.Warn("ws broadcast channel full")
	}
}

// OnlineCount 返回当前在线连接数。
func (h *WebSocketHub) OnlineCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	n := 0
	for _, clients := range h.clients {
		n += len(clients)
	}
	return n
}

// ==========================================
// Upgrader 与 Handler
// ==========================================

var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// WSHandler WebSocket 升级 HTTP 处理器。
type WSHandler struct {
	hub *WebSocketHub
}

// NewWSHandler 创建 WebSocket 处理器。
func NewWSHandler(hub *WebSocketHub) *WSHandler {
	return &WSHandler{hub: hub}
}

// Upgrade 升级 HTTP 连接为 WebSocket 连接。
// 需要 Auth 中间件前置注入 user_id。
func (h *WSHandler) Upgrade(c *gin.Context) {
	userID := c.GetString("user_id")
	if userID == "" || userID == "anonymous" {
		c.JSON(http.StatusUnauthorized, gin.H{"code": 10102, "message": "unauthorized"})
		c.Abort()
		return
	}

	conn, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		zap.L().Warn("ws upgrade failed", zap.String("user", userID), zap.Error(err))
		return
	}

	client := newWSClient(h.hub, conn, userID)
	h.hub.register <- client

	zap.L().Info("ws connected",
		zap.String("user", userID),
		zap.String("remote", conn.RemoteAddr().String()),
	)

	go client.writePump(zap.L())
	go client.readPump(zap.L())
}
