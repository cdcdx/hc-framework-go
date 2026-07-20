// ============================================================================
// accounts.js — k6 压测统一测试账号管理
//
// 背景：此前各压测脚本（auth/idle/mixed/shop/shop_flash/idle_settle/ws）各自拼接
// 邮箱、各自 sha256 密码、各自解析 token，规则散乱（前缀不一、后缀有时加 Date.now
// 有时不加、域名混用 example.com/test.com）。结果：
//   1. 跨脚本账号互相污染，DB 里脏账号无法统一清理；
//   2. 重跑时复用相同 email + 旧密码/登录失败累计 → 触发账号安全锁定(10203) 导致登录失败；
//   3. token 解析逻辑重复且易错（历史版本曾写错 data.token / data.access_token）。
//
// 本模块统一：
//   - 邮箱格式：k6test_<RUN_ID>_<prefix>_<seq>@example.com
//       · k6test_ 前缀：单一可识别标记，DB 清理一条 SQL 即可（见底部说明）；
//       · RUN_ID：进程级唯一（Date.now()），保证每次运行的账号互不碰撞，重跑不锁号；
//       · prefix：脚本语义标识（ws/idle/mixed/shop/...）；seq：账号序号。
//   - 密码：统一 sha256(明文)，与服务端 auth 约定一致（密码字段收 SHA256 而非明文）。
//   - token：统一从 { code, data:{ access_token } } 解析，多路径兜底。
//
// 使用：
//   import { makeEmail, TEST_PASSWORD, passwordHash, loginUser, parseToken } from './accounts.js';
//   const email = makeEmail('ws', vu);            // 生成账号
//   const token = loginUser(email);               // 注册+登录取 token（注册冲突视为成功）
// ============================================================================

import { check } from 'k6';
import http from 'k6/http';
import { createHash } from 'k6/crypto';

// 运行级唯一 ID：每个 k6 进程一次，保证本次运行的所有脚本账号互不碰撞。
export const RUN_ID = Date.now();

// 统一域名与标记前缀
const EMAIL_DOMAIN = 'example.com';
const EMAIL_TAG = 'k6test';

// 测试账号统一明文密码（满足服务端强密码规则：大小写+数字+特殊字符）
export const TEST_PASSWORD = 'TestPass123!';

// 生成统一格式邮箱
export function makeEmail(prefix, seq) {
  return `${EMAIL_TAG}_${RUN_ID}_${prefix}_${seq}@${EMAIL_DOMAIN}`;
}

// SHA256(明文密码) —— 服务端 auth 约定 password 字段收哈希值
export function passwordHash(plain = TEST_PASSWORD) {
  const h = createHash('sha256');
  h.update(plain);
  return h.digest('hex');
}

// 从登录响应体解析 access_token（兼容 data.access_token / data.token / data.data.access_token）
export function parseToken(res) {
  if (!res || res.status !== 200) return null;
  let body;
  try {
    body = res.json();
  } catch (e) {
    return null;
  }
  const d = body && body.data;
  return (
    (d && (d.access_token || d.token)) ||
    (d && d.data && (d.data.access_token || d.data.token)) ||
    (body && (body.access_token || body.token)) ||
    null
  );
}

// 注册一个账号（忽略“邮箱已注册”冲突，因密码确定性一致，冲突即视为已存在可用）
export function registerUser(email, plain = TEST_PASSWORD) {
  const res = http.post(
    `${__ENV.BASE_URL || 'http://localhost:8080'}/api/v1/auth/register`,
    JSON.stringify({ email, password: passwordHash(plain) }),
    { headers: { 'Content-Type': 'application/json' }, tags: { name: 'setup_register' } },
  );
  return res;
}

// 注册 + 登录，返回 token 字符串；失败返回 null（并打印原因，便于排查 auth 链路）
export function loginUser(email, plain = TEST_PASSWORD) {
  registerUser(email, plain);
  const res = http.post(
    `${__ENV.BASE_URL || 'http://localhost:8080'}/api/v1/auth/login`,
    JSON.stringify({ email, password: passwordHash(plain) }),
    { headers: { 'Content-Type': 'application/json' }, tags: { name: 'setup_login' } },
  );
  const token = parseToken(res);
  if (!token) {
    console.error(`[accounts] login failed for ${email}: status=${res && res.status} body=${JSON.stringify(res && res.json && res.json())}`);
  }
  return token;
}

// 批量注册并登录，返回 [{ email, token }]，自动跳过无 token 的账号。
// 适合 setup() 中一次构造账号池；register 冲突(10201)被忽略。
export function createUserPool(prefix, size, plain = TEST_PASSWORD) {
  const out = [];
  for (let i = 0; i < size; i++) {
    const email = makeEmail(prefix, i);
    const token = loginUser(email, plain);
    if (token) out.push({ email, token });
  }
  return out;
}

// ----------------------------------------------------------------------------
// 清理说明：每次运行会在 DB 写入以 k6test_ 开头的账号（约数千~上万条，依脚本而定）。
// 重跑无需手动清理（RUN_ID 保证不碰撞、不锁号），但长期累积需清理时可执行：
//
//   DELETE FROM users WHERE email LIKE 'k6test_%';
//
// 若该表有级联外键（token/会话/积分等）请一并清理对应表，或改用软删除/测试库。
// ----------------------------------------------------------------------------
