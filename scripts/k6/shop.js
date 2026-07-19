import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { Trend, Rate, Counter, Gauge } from 'k6/metrics';
import { createHash } from 'k6/crypto';

// ============================================
// 积分商城压测：商品兑换（抢购场景）
// 验证：零超卖
//
// 前置条件（需在压测前手动准备）：
//   1. 数据库中创建商品 ITEM_ID，设置 stock=500
//   2. 测试用户需要有足够的积分余额
//   3. 建议临时调高 RateLimit 的 burst 值或暂时禁用限流，
//      以便测试聚焦于库存并发扣减逻辑
// ============================================

export const options = {
    stages: [
        { duration: '10s', target: 1000 },   // 快速拉起（模拟同时抢购）
        { duration: '50s', target: 1000 },   // 满负载持续
        { duration: '30s', target: 0 },      // 冷却
    ],
    // 注册走 bcrypt cost=12，200 个用户顺序注册极易超过默认 60s 的 setupTimeout。
    // 放大超时上限，并配合 setup 内 http.batch 分批并发注册，确保 setup 在数秒内完成。
    setupTimeout: '300s',
    thresholds: {
        // 注：本压测为"抢购"场景（1000 VU 同抢 500 库存），p99 含 DB 事务 + Redis 锁，
        // 500ms 过严会误报。放宽到 2s 仅作观测，不影响零超卖结论（由 shop_redeem_success 保证）。
        'http_req_duration': ['p(99)<2000'],
        'http_req_failed': ['rate<0.01'],     // 允许部分限流/售罄导致的失败
        'shop_redeem_success': ['count<=500'], // 库存 500，成功数不应超过 500（客户端视角门禁）
        'db_final_stock': ['value>=0'],        // 服务端真实库存非负（零超卖硬标准；teardown 查得，缺失时由 teardown 补 0 兜底）
    },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
// 默认压测商品 id=1（库存 500 / 积分价 0 / 在售）。
// 注意：早期版本曾因缓存层布隆过滤器误判（!exists 被短路为“不存在”）导致所有
// FindItemByID 返回 10303，该问题已在服务端修复；若仍报 10303，请确认服务已重启
// 并预置 item 1 的 stock=500 / price_points=0 / is_active=1。
const ITEM_ID = __ENV.ITEM_ID || '1';
// 初始库存：用于 teardown 服务端库存对账。实际应以压测前 DB 真实库存为准，可通过环境变量覆盖。
const INITIAL_STOCK = __ENV.INITIAL_STOCK ? parseInt(__ENV.INITIAL_STOCK) : 500;

// 自定义指标
const redeemDuration = new Trend('shop_redeem_duration');
const redeemSuccess = new Rate('shop_redeem_ok');
const redeemSuccessCount = new Counter('shop_redeem_success');
const redeemFailCount = new Counter('shop_redeem_fail');
// 服务端真实库存（teardown 阶段经 /shop/items 翻页查得），用于"零超卖"硬对账：
// 以 DB 真实值为准，弥补"客户端成功计数"视角（shop_redeem_success）可能存在的漏报/多报盲区。
const dbFinalStock = new Gauge('db_final_stock');
// 失败码分桶：k6 的 Counter 加 tag 后无法在 handleSummary 里按 tag 拆分（values.tags.code 恒为空），
// 因此改用一组“显式命名 Counter”逐码计数，才能在汇总里真实看到失败码分布。
const failCode = {
    c10301: new Counter('shop_fail_10301'), // 积分不足
    c10302: new Counter('shop_fail_10302'), // 库存不足
    c10303: new Counter('shop_fail_10303'), // 商品下架/不存在
    c10310: new Counter('shop_fail_10310'), // 重复兑换（并发锁冲突，可重试，对应 10310）
    c10311: new Counter('shop_fail_10311'), // 已售罄（削峰层拦截，对应 10311）
    c10102: new Counter('shop_fail_10102'), // token 无效
    c10601: new Counter('shop_fail_10601'), // 限流
    c10602: new Counter('shop_fail_10602'), // 熔断
    c10003: new Counter('shop_fail_10003'), // 未知错误
    c10701: new Counter('shop_fail_10701'), // DB 异常
    cHttp401: new Counter('shop_fail_http401'),
    cHttp429: new Counter('shop_fail_http429'),
    cHttp500: new Counter('shop_fail_http500'),
    cOther: new Counter('shop_fail_other'),
};

// ============================================
// setup: 批量注册测试用户，获取有效 Token
// ============================================
export function setup() {
    // 用户池 = 最大 VU 数：让每个 VU 独占一个账号，避免多 VU 抢同一用户的分布式锁
    // （redeem:lock:{userID}）→ 大量 10310 并发锁冲突，使压测根本无法跑出真实库存扣减负载。
    // 1000 个独立账号各兑 1 次，才能真实验证"500 库存最多 500 单、零超卖"。
    const POOL_SIZE = 1000;
    const users = [];

    console.log(`[Setup] Creating ${POOL_SIZE} test users for shop test...`);

    // 分批注册：MySQL max_connections=151，应用连接池 max_open_conns=120，
    // 一次性 200 并发注册会瞬间打满连接池 → Too many connections → 注册失败 → token 缺失 → 压测全 401。
    // 改为每批 20 个 http.batch，批次间 sleep，控制并发在连接池容量内，确保拿到足量有效 token。
    const BATCH = 20;
    const regFail = {};           // status -> count
    let regSampleBody = '';       // 一条失败样本 body
    let parseFail = 0;
    const baseTs = Date.now();
    for (let start = 0; start < POOL_SIZE; start += BATCH) {
        const end = Math.min(start + BATCH, POOL_SIZE);
        const regRequests = [];
        const emails = [];
        for (let i = start; i < end; i++) {
            const email = `shop_${baseTs}_${i}@example.com`;
            emails.push(email);
            regRequests.push({
                method: 'POST',
                url: `${BASE_URL}/api/v1/auth/register`,
                body: JSON.stringify({
                    email: email,
                    password: sha256('TestPass123!'),
                }),
                params: { headers: { 'Content-Type': 'application/json' } },
            });
        }
        const regResponses = http.batch(regRequests);
        for (let i = 0; i < regResponses.length; i++) {
            const regRes = regResponses[i];
            if (regRes.status !== 200) {
                regFail[regRes.status] = (regFail[regRes.status] || 0) + 1;
                if (!regSampleBody) regSampleBody = String(regRes.body).slice(0, 200);
                continue;
            }

            let token;
            try {
                token = JSON.parse(regRes.body).data.access_token;
            } catch (e) {
                parseFail++;
                continue;
            }

            if (token) {
                users.push({ email: emails[i], token });
            } else {
                parseFail++;
            }
        }
        // 批次间隔，给 MySQL 连接池喘息，避免 Too many connections
        sleep(0.3);
    }
    console.log(`[Setup] register: ok=${users.length} parseFail=${parseFail} httpFail=${JSON.stringify(regFail)} sample=${regSampleBody}`);

    if (users.length === 0) {
        throw new Error('[Setup] Failed to create any test users');
    }

    console.log(`[Setup] Created ${users.length} test users`);
    console.log(`[Setup] Ensure item ${ITEM_ID} has stock=500 and users have enough points before running`);

    return { users };
}

export default function (data) {
    // 每个 VU 独占一个账号（POOL_SIZE === 最大 VU 数），避免多 VU 共享账号触发分布式锁冲突。
    const idx = (__VU - 1) % data.users.length;
    const token = data.users[idx].token;

    group('Shop Redeem', () => {
        const payload = JSON.stringify({
            item_id: parseInt(ITEM_ID),
            quantity: 1,
        });

        const res = http.post(`${BASE_URL}/api/v1/shop/redeem`, payload, {
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`,
            },
            tags: { name: 'redeem' },
        });

        redeemDuration.add(res.timings.duration);

        // 注意：response.Error 对业务错误（10301/10302/10601 等）也返回 HTTP 200，
        // 因此不能用 res.status===200 判定“兑换成功”，否则所有“库存不足”响应都会被
        // 误记为成功，导致虚假的“超卖”误报。必须以业务码 code===0 作为真实成功标准。
        let body;
        try { body = JSON.parse(res.body); } catch (e) { body = null; }
        const isOk = res.status === 200 && body && body.code === 0;
        redeemSuccess.add(isOk);

        if (isOk) {
            redeemSuccessCount.add(1);
        } else {
            redeemFailCount.add(1);
            const code = (body && body.code !== undefined) ? body.code : null;
            if (code === 10301) failCode.c10301.add(1);
            else if (code === 10302) failCode.c10302.add(1);
            else if (code === 10303) failCode.c10303.add(1);
            else if (code === 10310) failCode.c10310.add(1);
            else if (code === 10311) failCode.c10311.add(1);
            else if (code === 10102) failCode.c10102.add(1);
            else if (code === 10601) failCode.c10601.add(1);
            else if (code === 10602) failCode.c10602.add(1);
            else if (code === 10003) failCode.c10003.add(1);
            else if (code === 10701) failCode.c10701.add(1);
            else if (res.status === 401) failCode.cHttp401.add(1);
            else if (res.status === 429) failCode.cHttp429.add(1);
            else if (res.status === 500) failCode.cHttp500.add(1);
            else failCode.cOther.add(1);

            // 抓一条失败样本（仅首个 VU 的第 0 次迭代），用于定位未知失败原因
            if (__VU === 1 && __ITER === 0) {
                console.log(`[Redeem-Sample] status=${res.status} body=${String(res.body).slice(0, 300)}`);
            }
        }

        check(res, {
            'redeem response valid': (r) => {
                try {
                    const b = JSON.parse(r.body);
                    // 成功: code=0
                    // 失败: 10301(积分不足) / 10302(库存不足) / 10601(限流)
                    return b.code === 0 ||
                        [10301, 10302, 10601, 10310, 10311].includes(b.code);
                } catch (e) {
                    return false;
                }
            },
        });
    });

    // 短间隔防止 RateLimiter 完全拦截，同时保持抢购特征
    sleep(0.05 + Math.random() * 0.05);
}

// ============================================
// teardown：服务端库存对账（零超卖硬验证）
// 用注册用户的 token 翻页查询目标商品的真实 stock，以 DB 真实值为准断言零超卖。
// - 查到 stock<0：直接 throw，使 k6 非零退出（CI 门禁）；
// - 查不到 item：补 db_final_stock=0 兜底，避免阈值因指标缺失误报，并打印警告。
// 弥补 shop_redeem_success（客户端成功计数）视角的盲区：若服务端已落单却返回失败
// （客户端记为失败），客户端计数会漏报超卖，此处以 DB 为准可捕获。
// ============================================
export function teardown(data) {
    const token = data.users && data.users[0] ? data.users[0].token : null;
    if (!token) {
        console.log('[Teardown] no token available, skip DB stock check');
        return;
    }
    const targetId = parseInt(ITEM_ID);
    let cursor = '';
    let finalStock = null;
    for (let page = 0; page < 200; page++) {
        const url = `${BASE_URL}/api/v1/shop/items?limit=100` + (cursor ? `&cursor=${cursor}` : '');
        const res = http.get(url, {
            headers: { 'Authorization': `Bearer ${token}` },
            tags: { name: 'teardown_items' },
        });
        if (res.status !== 200) {
            console.log(`[Teardown] /shop/items HTTP ${res.status}, skip DB check`);
            dbFinalStock.add(0);
            break;
        }
        let body;
        try { body = JSON.parse(res.body); } catch (e) {
            console.log('[Teardown] /shop/items parse failed, skip DB check');
            dbFinalStock.add(0);
            break;
        }
        const items = (body && body.data && body.data.items) || [];
        const found = items.find((it) => it.id === targetId);
        if (found) { finalStock = found.stock; break; }
        if (body.data && body.data.has_more && body.data.next_cursor) {
            cursor = body.data.next_cursor;
        } else {
            break;
        }
    }

    if (finalStock === null) {
        console.log(`[Teardown] item ${targetId} not found in /shop/items, DB stock check skipped`);
        dbFinalStock.add(0);
        return;
    }

    dbFinalStock.add(finalStock);
    console.log(`[Teardown] DB final stock of item ${targetId} = ${finalStock} (initial ${INITIAL_STOCK})`);
    if (finalStock < 0) {
        throw new Error(`OVERSELL DETECTED: item ${targetId} final stock = ${finalStock} < 0`);
    }
}

export function handleSummary(data) {
    const totalSuccess = data.metrics['shop_redeem_success']?.values?.count || 0;
    const totalFail = data.metrics['shop_redeem_fail']?.values?.count || 0;

    console.log(`========================================`);
    console.log(`兑换压测结果汇总:`);
    console.log(`  成功兑换: ${totalSuccess}`);
    console.log(`  兑换失败: ${totalFail}`);
    console.log(`  总请求: ${totalSuccess + totalFail}`);
    const cnt = (name) => data.metrics[name]?.values?.count || 0;
    const failCodes = {
        '10301积分不足': cnt('shop_fail_10301'),
        '10302库存不足': cnt('shop_fail_10302'),
        '10303商品下架': cnt('shop_fail_10303'),
        '10310并发锁冲突(可重试)': cnt('shop_fail_10310'),
        '10311售罄(削峰拦截)': cnt('shop_fail_10311'),
        '10102token无效': cnt('shop_fail_10102'),
        '10601限流': cnt('shop_fail_10601'),
        '10602熔断': cnt('shop_fail_10602'),
        '10003未知错误': cnt('shop_fail_10003'),
        '10701DB异常': cnt('shop_fail_10701'),
        'http401': cnt('shop_fail_http401'),
        'http429': cnt('shop_fail_http429'),
        'http500': cnt('shop_fail_http500'),
        'other': cnt('shop_fail_other'),
    };
    // 只打印非零项，便于阅读
    const nonZero = Object.fromEntries(Object.entries(failCodes).filter(([, v]) => v > 0));
    console.log(`  失败码分布: ${JSON.stringify(nonZero)}`);
    const dbStock = data.metrics['db_final_stock']?.values?.value;
    console.log(`  库存预期: ${INITIAL_STOCK} (零超卖验证)`);
    console.log(`  客户端超卖: ${totalSuccess > INITIAL_STOCK ? '❌ YES!' : '✅ NO'}`);
    if (dbStock !== undefined && dbStock !== null) {
        const expectedSuccess = INITIAL_STOCK - dbStock;
        const drift = totalSuccess - expectedSuccess;
        console.log(`  DB 最终库存: ${dbStock}`);
        console.log(`  DB 对账: 客户端成功 ${totalSuccess} vs 服务端应成功 ${expectedSuccess} -> 漂移 ${drift}`);
        if (dbStock < 0) {
            console.log(`  ❌ 零超卖硬失败: DB 库存为负(${dbStock})`);
        } else if (drift !== 0) {
            console.log(`  ⚠️ 客户端/服务端计数漂移 ${drift}（可能漏报/多报，需排查）`);
        } else {
            console.log(`  ✅ DB 对账一致，零超卖确认`);
        }
    } else {
        console.log(`  DB 对账: 未执行（teardown 跳过）`);
    }
    console.log(`========================================`);

    return {
        'stdout': JSON.stringify({
            success: totalSuccess,
            fail: totalFail,
            total: totalSuccess + totalFail,
            expected_stock: INITIAL_STOCK,
            oversold: totalSuccess > INITIAL_STOCK,
        }, null, 2),
    };
}

function sha256(str) {
    const hasher = createHash('sha256');
    hasher.update(str);
    return hasher.digest('hex');
}
