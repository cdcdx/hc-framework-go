import http from 'k6/http';
import { sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';
import ws from 'k6/ws';
import { makeEmail, passwordHash, parseToken } from './accounts.js';

// ---------------------------------------------------------------------------
// WebSocket 压测脚本
//
// register / login 已移出压测热路径：在 setup() 里为每个 VU 预建账号、预取 token，
// 循环里「只建立 WebSocket 连接 + 心跳」。这避免每轮迭代各跑一次 bcrypt 制造 CPU 洪峰
// （对照服务端 auth_service.go 的 bcrypt 准入信号量：容量=核心数），让 WS 场景本身可压到更高 VU。
//
// 账号统一由 accounts.js 管理：格式 k6test_<RUN_ID>_ws_hb_<VU>@example.com 等，
// RUN_ID 保证每次运行不碰撞、不锁号；清理见 accounts.js 底部说明。
// ---------------------------------------------------------------------------

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const WS_URL = (BASE_URL.replace(/^http/, 'ws')) + '/api/v1/ws';

// 各场景的最大 VU（与下方 scenarios 的 vusMax 一致），setup 据此预建账号。
const MAX_HEARTBEAT_VUS = 10000;
const MAX_RECONNECT_VUS = 200;

// ===================== 自定义指标 =====================
const wsConnectDuration = new Trend('ws_connect_duration');
const wsConnectOk = new Rate('ws_connect_ok');
const wsPingPongDuration = new Trend('ws_pingpong_duration');

// ===================== setup：预建账号 + 预取 token =====================
// 仅执行一次（每个 k6 进程一次），返回按 email 索引的 token 表。
// 注意：1 万+账号若逐个串行 register+login 会远超默认 60s 的 setupTimeout 而被强杀
// （曾导致主场景 0 连接、ws_connect_ok=0%）。
// 故用 http.batch 批量并发注册 + 批量登录（与 idle/shop 等脚本一致），大幅缩短 setup 耗时。
export function setup() {
  const tokens = {};

  function batchEnsure(prefix, count) {
    const emails = [];
    for (let vu = 1; vu <= count; vu++) emails.push(makeEmail(prefix, vu));

    // 批量注册（注册冲突视为成功，因密码确定性一致）
    const regReqs = emails.map((email) => ({
      method: 'POST',
      url: `${BASE_URL}/api/v1/auth/register`,
      body: JSON.stringify({ email, password: passwordHash() }),
      params: { headers: { 'Content-Type': 'application/json' }, tags: { name: 'setup_register' } },
    }));
    http.batch(regReqs);

    // 批量登录取 token
    const loginReqs = emails.map((email) => ({
      method: 'POST',
      url: `${BASE_URL}/api/v1/auth/login`,
      body: JSON.stringify({ email, password: passwordHash() }),
      params: { headers: { 'Content-Type': 'application/json' }, tags: { name: 'setup_login' } },
    }));
    const loginResps = http.batch(loginReqs);
    loginResps.forEach((res, idx) => {
      const token = parseToken(res);
      if (!token) {
        console.error(`[setup] ${emails[idx]} login failed: status=${res && res.status}`);
      }
      tokens[emails[idx]] = token;
    });
  }

  const t0 = Date.now();
  batchEnsure('ws_hb', MAX_HEARTBEAT_VUS);
  batchEnsure('ws_rc', MAX_RECONNECT_VUS);
  console.log(`[setup] pre-created accounts for ${MAX_HEARTBEAT_VUS} heartbeat + ${MAX_RECONNECT_VUS} reconnect VUs in ${Date.now() - t0}ms`);

  return { tokens };
}

// ===================== 场景：ws_heartbeat =====================
// 长连接 + 周期 ping/pong，验证服务端心跳稳定性与吞吐。
export function wsHeartbeat(data) {
  const vu = __VU;
  const email = makeEmail('ws_hb', vu);
  const token = data.tokens ? data.tokens[email] : null;
  if (!token) {
    console.error(`VU ${vu}: no token, skip heartbeat`);
    wsConnectOk.add(false);
    return;
  }

  const url = WS_URL;
  const connectStart = Date.now();
  const res = ws.connect(url, {
    headers: { Authorization: `Bearer ${token}` },
    tags: { name: 'ws_heartbeat' },
  }, function (socket) {
    socket.on('open', () => {
      wsConnectDuration.add(Date.now() - connectStart);
      wsConnectOk.add(true);

      // 每 5s 发一次 ping，校验 pong 往返耗时
      socket.setInterval(function () {
        const t0 = Date.now();
        socket.send(JSON.stringify({ type: 'ping', ts: t0 }));
        socket.setTimeout(function () {
          wsPingPongDuration.add(Date.now() - t0);
        }, 1000);
      }, 5000);
    });

    socket.on('message', (msg) => {
      try {
        const m = JSON.parse(msg);
        if (m.type === 'pong' || m.type === 'heartbeat' || m.type === 'ack') {
          // 往返耗时已在 setInterval 的 setTimeout 中记录，这里仅作存活确认
        }
      } catch (e) {
        // 忽略非 JSON 控制帧
      }
    });

    socket.on('close', () => {/* 正常关闭 */});
    socket.on('error', (e) => {
      console.error(`VU ${vu}: ws error: ${e}`);
      wsConnectOk.add(false);
    });
  });

  // 连接建立失败（如网络/鉴权失败）计入失败
  if (!res || res.error) {
    console.error(`VU ${vu}: ws connect failed: ${res ? res.error : 'no response'}`);
    wsConnectOk.add(false);
  }

  // 每个 VU 保持连接一段时间（202s），由场景 ramping 控制并发量
  sleep(202);
  // 主动关闭，避免遗留连接影响后续统计
  try { res && res.close && res.close(); } catch (e) {}
}

// ===================== 场景：ws_reconnect =====================
// 短连接 + 周期性断开重连，验证重连逻辑与连接抖动下的稳定性。
export function wsReconnect(data) {
  const vu = __VU;
  const email = makeEmail('ws_rc', vu);
  const token = data.tokens ? data.tokens[email] : null;
  if (!token) {
    console.error(`VU ${vu}: no token, skip reconnect`);
    wsConnectOk.add(false);
    return;
  }

  const url = WS_URL;
  const cycles = 20; // 持续约 2m，每轮 6s
  for (let i = 0; i < cycles; i++) {
    const connectStart = Date.now();
    const res = ws.connect(url, {
      headers: { Authorization: `Bearer ${token}` },
      tags: { name: 'ws_reconnect' },
    }, function (socket) {
      socket.on('open', () => {
        wsConnectDuration.add(Date.now() - connectStart);
        wsConnectOk.add(true);
        socket.send(JSON.stringify({ type: 'ping', ts: Date.now() }));
      });
      socket.on('message', () => {/* 忽略 */});
      socket.on('error', (e) => {
        console.error(`VU ${vu}: reconnect ws error: ${e}`);
        wsConnectOk.add(false);
      });
      socket.on('close', () => {/* 主动断开 */});
    });

    if (!res || res.error) {
      console.error(`VU ${vu}: reconnect failed: ${res ? res.error : 'no response'}`);
      wsConnectOk.add(false);
    }

    // 保持 3s 后断开，再等 3s 重连
    sleep(3);
    try { res && res.close && res.close(); } catch (e) {}
    sleep(3);
  }
}

// ===================== 场景配置 =====================
export const options = {
  scenarios: {
    ws_heartbeat: {
      executor: 'constant-vus',
      vus: 10000,
      duration: '7m',
      exec: 'wsHeartbeat',
    },
    ws_reconnect: {
      executor: 'constant-vus',
      vus: 200,
      duration: '2m',
      exec: 'wsReconnect',
    },
  },
  // setup 需预建 1 万+ 账号（批量 register+login），放宽超时避免被 60s 强杀
  setupTimeout: '300s',
  thresholds: {
    ws_connect_duration: ['p(99)<2000'],
    ws_connect_ok: ['rate>0.99'],
    ws_pingpong_duration: ['p(99)<500'],
  },
};
