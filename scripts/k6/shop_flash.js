import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { Trend, Rate, Counter, Gauge } from 'k6/metrics';
import { makeEmail, passwordHash } from './accounts.js';

// 仅用于 WS 指标分组名，便于区分不同运行（与账号无关，账号统一走 accounts.js）
const baseTs = Date.now();

// ============================================
// 定时抢购压测：/api/v1/shop/flash/redeem
// 验证：零超卖（活动 sold_qty <= limit_qty）+ 削峰缓存拦截（大量请求在 Redis 层即被 10307 挡下，不触达 DB）
//
// 与 shop.js（普通积分兑换）的差异：
//   1. 打的是 /api/v1/shop/flash/redeem，body 为 { activity_id }；
//   2. 限量来自「活动 limit_qty」（非商品 stock），每人限购来自「活动 per_user_limit」；
//   3. 有两道防线：Redis 原子预扣（削峰，失败 10307/10308）+ DB 权威配额（兜底，sold_qty<limit_qty 才 +1）；
//   4. 零超卖硬标准为「活动 sold_qty <= limit_qty」（teardown 经运营详情接口查得）。
//
// 前置条件（压测前准备）：
//   1. 配置 admin.token（config.admin.token），并通过环境变量 ADMIN_TOKEN 传入；
//      脚本 setup 阶段将调用 POST /api/v1/admin/flash/activities 自动创建活动（limit_qty=500）。
//      —— 若不想走 token，可预先手动建好活动，用 FLASH_ACTIVITY_ID 指定并跳过创建。
//   2. 商品 FLASH_ITEM_ID（默认复用 ITEM_ID=1）需 is_active=1、stock>=500（抢购成功会扣商品库存，
//      但前 500 单成功、其后 10307 被挡，故 stock>=500 即可；建议设 1000 留余量）。
//   3. 建议临时调高 RateLimit 的 burst 或禁用限流，聚焦库存并发扣减逻辑。
// ============================================

// 峰值并发：可用 FLASH_VUS 环境变量覆盖（默认 1000，维持原破坏性负载）；各阶梯按比例缩放。
const FLASH_VUS = parseInt(__ENV.FLASH_VUS || '1000', 10);

export const options = {
    stages: [
        { duration: '10s', target: FLASH_VUS },   // 快速拉起（模拟同时抢购）
        { duration: '50s', target: FLASH_VUS },   // 满负载持续
        { duration: '30s', target: 0 },           // 冷却
    ],
    // 注册走 bcrypt cost=12，放大 setup 超时并分批并发注册（同 shop.js）。
    setupTimeout: '300s',
    thresholds: {
        // 抢购含 Redis 预扣 + DB 事务，p99 放宽到 2s 仅作观测，零超卖由 flash_redeem_success 保证。
        'http_req_duration': ['p(99)<2000'],
        'http_req_failed': ['rate<0.01'],     // 允许部分限流/售罄导致的失败
        'flash_redeem_success': ['count<=500'], // limit_qty=500，成功数不应超过 500（客户端视角门禁）
        'db_final_sold_qty': ['value>=0', 'value<=500'], // 服务端权威 sold_qty 非负且不超过限量（零超卖硬标准）
    },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
// 默认压测商品 id=1（在售 / stock>=500 / price_points 任意，抢购价由活动 price_points 覆盖）。
const ITEM_ID = __ENV.ITEM_ID || '1';
const FLASH_ITEM_ID = __ENV.FLASH_ITEM_ID || ITEM_ID;
// 活动限量：零超卖门禁与 teardown 对账均以它为准。
const LIMIT_QTY = __ENV.LIMIT_QTY ? parseInt(__ENV.LIMIT_QTY) : 500;
// 抢购价：默认 0（不扣积分，聚焦「先到先得不超卖」；如需验证积分不足 10301，可设 >0 并确保用户积分不足）。
const PRICE_POINTS = __ENV.PRICE_POINTS ? parseInt(__ENV.PRICE_POINTS) : 0;
// 每人限购：默认 1（每账号仅能成单 1 次，重复抢返回 10308）。
const PER_USER_LIMIT = __ENV.PER_USER_LIMIT ? parseInt(__ENV.PER_USER_LIMIT) : 1;
// 运营令牌：用于 setup 创建活动；缺省时须通过 FLASH_ACTIVITY_ID 指定已存在活动。
const ADMIN_TOKEN = __ENV.ADMIN_TOKEN || '';
const FLASH_ACTIVITY_ID = __ENV.FLASH_ACTIVITY_ID || '';
// 活动结束时间：默认空（不限结束）。非空时需 RFC3339 字符串，如 2026-07-16T20:30:00Z。
const END_TIME = __ENV.END_TIME || '';
// 活动开始时间偏移（秒）：默认 -1（过去 1 秒），活动创建后立即进入「销售中(on_sale)」，
// 使主压测阶段活动稳定可抢，真正压到抢购并发竞争（避免大量请求被 10305 未开始拦截）。
// 设为正值（如 8）可保留 presale→on_sale 状态机生命周期观测。
const FLASH_START_OFFSET = __ENV.FLASH_START_OFFSET !== undefined ? parseInt(__ENV.FLASH_START_OFFSET) : -1;

// 自定义指标
const redeemDuration = new Trend('flash_redeem_duration');
const redeemSuccess = new Rate('flash_redeem_ok');
const redeemSuccessCount = new Counter('flash_redeem_success');
const redeemFailCount = new Counter('flash_redeem_fail');
// 削峰层拦截观测：Redis 预扣返回的「已抢光」计数，应与（总请求 - 成功 - 其它）接近，
// 证明其不触达 DB 即拦下绝大多数超额请求（高并发削峰效果）。
const soldOutCount = new Counter('flash_redeem_sold_out');   // 10307
const userLimitCount = new Counter('flash_redeem_user_limit'); // 10308
// 服务端权威 sold_qty（teardown 经运营详情接口查得），用于零超卖硬对账。
// 用 Gauge 而非 Counter：阈值需按「值」断言（0<=sold_qty<=limit_qty），Counter 仅支持 count/rate。
const dbFinalSoldQty = new Gauge('db_final_sold_qty');
// 失败码分桶（同 shop.js：k6 按 tag 拆分不可靠，改用显式命名 Counter）。
const failCode = {
    c10301: new Counter('flash_fail_10301'), // 积分不足
    c10302: new Counter('flash_fail_10302'), // 库存不足（普通兑换；抢购一般不走此码）
    c10303: new Counter('flash_fail_10303'), // 商品下架/不存在
    c10310: new Counter('flash_fail_10310'), // 重复兑换（并发锁冲突，可重试，对应 10310）
    c10305: new Counter('flash_fail_10305'), // 抢购未开始
    c10306: new Counter('flash_fail_10306'), // 抢购已结束
    c10307: new Counter('flash_fail_10307'), // 已抢光（削峰层）
    c10308: new Counter('flash_fail_10308'), // 每人限购（削峰层）
    c10309: new Counter('flash_fail_10309'), // 抢购处理超时（尖峰 deadline 超时，请重试）
    c10002: new Counter('flash_fail_10002'), // 活动不存在
    c10102: new Counter('flash_fail_10102'), // token 无效
    c10601: new Counter('flash_fail_10601'), // 限流
    c10602: new Counter('flash_fail_10602'), // 熔断
    c10003: new Counter('flash_fail_10003'), // 未知错误
    c10701: new Counter('flash_fail_10701'), // DB 异常
    cHttp401: new Counter('flash_fail_http401'),
    cHttp403: new Counter('flash_fail_http403'), // admin token 缺失/错误
    cHttp429: new Counter('flash_fail_http429'),
    cHttp500: new Counter('flash_fail_http500'),
    cOther: new Counter('flash_fail_other'),
};
// 接口返回状态统计：对 /api/v1/shop/flash/activities 返回的每个活动的 sales_status 计数。
// 用于观测用户侧列表实际露出的状态分布（预售中/销售中/已卖完/已结束），验证状态派生与
// 生命周期校正生效。Counter 跨 VU 累加 = 整个压测中该状态的活动被返回的总次数。
const listStatus = {
    presale: new Counter('flash_list_status_presale'), // 预售中（定时前）
    on_sale: new Counter('flash_list_status_on_sale'),  // 销售中
    sold_out: new Counter('flash_list_status_sold_out'), // 已卖完
    ended: new Counter('flash_list_status_ended'),       // 已结束
    other: new Counter('flash_list_status_other'),       // 未知/缺失
};
// 模块级采样标志：首次出现 10003 未知错误时打印服务端真实错误体（含 err.Error()），
// 用于定位并发尖峰下的根因（DB 连接池打满 / Redis 异常等）。k6 单 JS VM 共享该变量，竞态无碍。
let gUnknownSampleLogged = false;

// ============================================
// setup：批量注册测试用户 + 经运营接口创建抢购活动
// ============================================
export function setup() {
    // 用户池 = 最大 VU 数：每账号各抢 1 次，才能真实验证「limit_qty 库存最多 limit_qty 单、零超卖」。
    const POOL_SIZE = 1000;
    const users = [];

    console.log(`[Setup] Creating ${POOL_SIZE} test users for flash sale test...`);

    // 分批注册（同 shop.js：控制并发在 MySQL 连接池内，避免 Too many connections）。
    const BATCH = 20;
    const regFail = {};
    let regSampleBody = '';
    let parseFail = 0;
    for (let start = 0; start < POOL_SIZE; start += BATCH) {
        const end = Math.min(start + BATCH, POOL_SIZE);
        const regRequests = [];
        const emails = [];
        for (let i = start; i < end; i++) {
            const email = makeEmail('flash', i);
            emails.push(email);
            regRequests.push({
                method: 'POST',
                url: `${BASE_URL}/api/v1/auth/register`,
                body: JSON.stringify({
                    email: email,
                    password: passwordHash(),
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
            try { token = JSON.parse(regRes.body).data.access_token; } catch (e) { parseFail++; continue; }
            if (token) users.push({ email: emails[i], token });
            else parseFail++;
        }
        sleep(0.3);
    }
    console.log(`[Setup] register: ok=${users.length} parseFail=${parseFail} httpFail=${JSON.stringify(regFail)} sample=${regSampleBody}`);

    if (users.length === 0) {
        throw new Error('[Setup] Failed to create any test users');
    }

    // ---- 准备抢购活动 ----
    let activityId = FLASH_ACTIVITY_ID ? parseInt(FLASH_ACTIVITY_ID) : 0;
    if (activityId > 0) {
        console.log(`[Setup] Reuse existing flash activity id=${activityId} (FLASH_ACTIVITY_ID)`);
    } else if (ADMIN_TOKEN) {
        // 经运营接口在 setup 阶段直接创建活动。start_time 由 FLASH_START_OFFSET 控制
        // （默认过去 1 秒 → 创建后即「销售中(on_sale)」），使主压测阶段活动稳定可抢，
        // 真正压到抢购并发竞争，避免大量请求被 10305 未开始拦截导致压测失真。
        // 需观测 presale→on_sale 状态机生命周期时，可设 FLASH_START_OFFSET=8（未来 8 秒）。
        // end_time 不传（空串会让 Go 的 time.Time 绑定失败）→ 不限结束；
        // 仅当 END_TIME 非空时才带该字段（RFC3339 字符串）。
        const actBody = {
            item_id: parseInt(FLASH_ITEM_ID),
            name: `k6_flash_${baseTs}`,
            start_time: new Date(Date.now() + FLASH_START_OFFSET * 1000).toISOString(),
            limit_qty: LIMIT_QTY,
            per_user_limit: PER_USER_LIMIT,
            price_points: PRICE_POINTS,
        };
        if (END_TIME) actBody.end_time = END_TIME;
        const body = JSON.stringify(actBody);
        const res = http.post(`${BASE_URL}/api/v1/admin/flash/activities`, body, {
            headers: { 'Content-Type': 'application/json', 'X-Admin-Token': ADMIN_TOKEN },
            tags: { name: 'setup_create_activity' },
        });
        if (res.status !== 200) {
            throw new Error(`[Setup] create flash activity failed: HTTP ${res.status} body=${String(res.body).slice(0, 300)}`);
        }
        let act;
        try { act = JSON.parse(res.body).data; } catch (e) { act = null; }
        if (!act || !act.id) {
            throw new Error(`[Setup] create flash activity returned no id: body=${String(res.body).slice(0, 300)}`);
        }
        activityId = act.id;
        console.log(`[Setup] Created flash activity id=${activityId} item=${FLASH_ITEM_ID} limit_qty=${LIMIT_QTY} price_points=${PRICE_POINTS}`);
    } else {
        throw new Error('[Setup] No activity: set ADMIN_TOKEN (to create) or FLASH_ACTIVITY_ID (to reuse)');
    }

    console.log(`[Setup] Ensure item ${FLASH_ITEM_ID} is_active=1 and stock>=${LIMIT_QTY} before running`);
    return { users, activityId };
}

export default function (data) {
    // 每个 VU 独占一个账号（POOL_SIZE === 最大 VU 数），避免多 VU 共享账号触发每人限购/并发锁。
    const idx = (__VU - 1) % data.users.length;
    const token = data.users[idx].token;
    const activityId = data.activityId;
    const authHeaders = {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`,
    };

    group('Flash Activity List (cache read)', () => {
        // 用户侧活动列表读（三级缓存 TTL 5s 削峰）：模拟「进页面看活动」，验证高并发读不回源 DB。
        const res = http.get(`${BASE_URL}/api/v1/shop/flash/activities?limit=20`, {
            headers: authHeaders,
            tags: { name: 'flash_list' },
        });
        check(res, {
            'flash list ok': (r) => {
                try { return JSON.parse(r.body).code === 0; } catch (e) { return false; }
            },
        });

        // 统计接口返回的活动销售状态数量（sales_status 由服务端派生，仅展示用）。
        try {
            const b = JSON.parse(res.body);
            if (b && Array.isArray(b.data)) {
                for (const a of b.data) {
                    const st = a.sales_status || 'other';
                    if (st === 'presale') listStatus.presale.add(1);
                    else if (st === 'on_sale') listStatus.on_sale.add(1);
                    else if (st === 'sold_out') listStatus.sold_out.add(1);
                    else if (st === 'ended') listStatus.ended.add(1);
                    else listStatus.other.add(1);
                }
            }
        } catch (e) { /* 解析失败不阻塞主流程 */ }
    });

    group('Flash Redeem', () => {
        const payload = JSON.stringify({ activity_id: activityId });
        const res = http.post(`${BASE_URL}/api/v1/shop/flash/redeem`, payload, {
            headers: authHeaders,
            tags: { name: 'flash_redeem' },
        });

        redeemDuration.add(res.timings.duration);

        // 业务成功以 code===0 为准（10307/10308 等也返回 HTTP 200），否则会误记超卖。
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
            else if (code === 10305) failCode.c10305.add(1);
            else if (code === 10306) failCode.c10306.add(1);
            else if (code === 10307) { failCode.c10307.add(1); soldOutCount.add(1); }   // 削峰层拦截
            else if (code === 10308) { failCode.c10308.add(1); userLimitCount.add(1); } // 削峰层拦截
            else if (code === 10309) {
                failCode.c10309.add(1);
                // 采样首次抢购超时，打印服务端真实错误体（含底层根因）以定位尖峰瓶颈。
                if (!gUnknownSampleLogged) {
                    gUnknownSampleLogged = true;
                    console.log(`[FlashRedeem-Timeout-Sample] status=${res.status} body=${String(res.body).slice(0, 400)}`);
                }
            }
            else if (code === 10002) failCode.c10002.add(1);
            else if (code === 10102) failCode.c10102.add(1);
            else if (code === 10601) failCode.c10601.add(1);
            else if (code === 10602) failCode.c10602.add(1);
            else if (code === 10003) {
                failCode.c10003.add(1);
                // 采样首次未知错误，打印服务端真实错误体以定位根因（连接池/Redis 等）。
                if (!gUnknownSampleLogged) {
                    gUnknownSampleLogged = true;
                    console.log(`[FlashRedeem-Unknown-Sample] status=${res.status} body=${String(res.body).slice(0, 400)}`);
                }
            }
            else if (code === 10701) failCode.c10701.add(1);
            else if (res.status === 401) failCode.cHttp401.add(1);
            else if (res.status === 403) failCode.cHttp403.add(1);
            else if (res.status === 429) failCode.cHttp429.add(1);
            else if (res.status === 500) failCode.cHttp500.add(1);
            else failCode.cOther.add(1);

            if (__VU === 1 && __ITER === 0) {
                console.log(`[FlashRedeem-Sample] status=${res.status} body=${String(res.body).slice(0, 300)}`);
            }
        }

        check(res, {
            'flash redeem response valid': (r) => {
                try {
                    const b = JSON.parse(r.body);
                    // 成功: code=0；失败（预期内）: 10307 已抢光 / 10308 每人限购 / 10601 限流
                    return b.code === 0 || [10307, 10308, 10601].includes(b.code);
                } catch (e) { return false; }
            },
        });
    });

    // 短间隔防止 RateLimiter 完全拦截，同时保持抢购特征
    sleep(0.05 + Math.random() * 0.05);
}

// ============================================
// teardown：服务端 sold_qty 对账（零超卖硬验证）
// 经运营详情接口查得活动真实 sold_qty，以 DB 权威值为准断言零超卖，弥补客户端计数盲区。
// 并演示一次库存对账自愈（POST .../sync），验证事故后 Redis 与 DB 对齐不丢单。
// ============================================
export function teardown(data) {
    const activityId = data.activityId;
    if (!activityId) {
        console.log('[Teardown] no activity id, skip DB check');
        return;
    }
    // 取一个用户 token 调运营接口（运营接口仅需 X-Admin-Token，用户 token 仅用于非运营字段无关；
    // 此处用 ADMIN_TOKEN 调详情接口）。
    const adminHeaders = { 'X-Admin-Token': ADMIN_TOKEN };
    if (!ADMIN_TOKEN) {
        console.log('[Teardown] ADMIN_TOKEN not set, skip server-side sold_qty check');
        return;
    }

    const res = http.get(`${BASE_URL}/api/v1/admin/flash/activities/${activityId}`, {
        headers: adminHeaders,
        tags: { name: 'teardown_activity_detail' },
    });
    if (res.status !== 200) {
        console.log(`[Teardown] GET activity detail HTTP ${res.status}, skip DB check`);
        return;
    }
    let act;
    try { act = JSON.parse(res.body).data; } catch (e) { act = null; }
    if (!act) {
        console.log('[Teardown] activity detail parse failed, skip DB check');
        return;
    }
    const soldQty = act.sold_qty;
    const limitQty = act.limit_qty;
    dbFinalSoldQty.add(soldQty);
    console.log(`[Teardown] activity ${activityId}: sold_qty=${soldQty} limit_qty=${limitQty}`);

    if (soldQty > limitQty) {
        throw new Error(`OVERSELL DETECTED: activity ${activityId} sold_qty=${soldQty} > limit_qty=${limitQty}`);
    }

    // 演示库存对账自愈：以 DB 权威值重置 Redis，验证幂等、不丢单（仅观测，不强制断言）。
    const syncRes = http.post(`${BASE_URL}/api/v1/admin/flash/activities/${activityId}/sync`, '', {
        headers: adminHeaders,
        tags: { name: 'teardown_sync' },
    });
    console.log(`[Teardown] sync reconcile HTTP ${syncRes.status} (idempotent, server-authoritative)`);
}

export function handleSummary(data) {
    const totalSuccess = data.metrics['flash_redeem_success']?.values?.count || 0;
    const totalFail = data.metrics['flash_redeem_fail']?.values?.count || 0;
    const soldOut = data.metrics['flash_redeem_sold_out']?.values?.count || 0;
    const userLimit = data.metrics['flash_redeem_user_limit']?.values?.count || 0;

    console.log(`========================================`);
    console.log(`定时抢购压测结果汇总:`);
    console.log(`  成功兑换: ${totalSuccess}`);
    console.log(`  兑换失败: ${totalFail}`);
    console.log(`  总请求: ${totalSuccess + totalFail}`);
    console.log(`  削峰层拦截(10307已抢光): ${soldOut}`);
    console.log(`  削峰层拦截(10308每人限购): ${userLimit}`);
    const cnt = (name) => data.metrics[name]?.values?.count || 0;
    const failCodes = {
        '10301积分不足': cnt('flash_fail_10301'),
        '10302库存不足': cnt('flash_fail_10302'),
        '10303商品下架': cnt('flash_fail_10303'),
        '10310并发锁冲突(可重试)': cnt('flash_fail_10310'),
        '10305未开始': cnt('flash_fail_10305'),
        '10306已结束': cnt('flash_fail_10306'),
        '10307已抢光': cnt('flash_fail_10307'),
        '10308每人限购': cnt('flash_fail_10308'),
        '10309抢购超时': cnt('flash_fail_10309'),
        '10002活动不存在': cnt('flash_fail_10002'),
        '10102token无效': cnt('flash_fail_10102'),
        '10601限流': cnt('flash_fail_10601'),
        '10602熔断': cnt('flash_fail_10602'),
        '10003未知错误': cnt('flash_fail_10003'),
        '10701DB异常': cnt('flash_fail_10701'),
        'http401': cnt('flash_fail_http401'),
        'http403': cnt('flash_fail_http403'),
        'http429': cnt('flash_fail_http429'),
        'http500': cnt('flash_fail_http500'),
        'other': cnt('flash_fail_other'),
    };
    const nonZero = Object.fromEntries(Object.entries(failCodes).filter(([, v]) => v > 0));
    console.log(`  失败码分布: ${JSON.stringify(nonZero)}`);
    const statusCnt = {
        presale: cnt('flash_list_status_presale'),
        on_sale: cnt('flash_list_status_on_sale'),
        sold_out: cnt('flash_list_status_sold_out'),
        ended: cnt('flash_list_status_ended'),
        other: cnt('flash_list_status_other'),
    };
    console.log(`  列表返回状态分布: ${JSON.stringify(statusCnt)}`);
    console.log(`  限量预期: ${LIMIT_QTY} (零超卖验证)`);
    console.log(`  客户端超卖: ${totalSuccess > LIMIT_QTY ? '❌ YES!' : '✅ NO'}`);
    const dbSold = data.metrics['db_final_sold_qty']?.values?.value;
    if (dbSold !== undefined && dbSold !== null) {
        const drift = totalSuccess - dbSold;
        console.log(`  DB 权威 sold_qty: ${dbSold}`);
        console.log(`  DB 对账: 客户端成功 ${totalSuccess} vs 服务端 sold_qty ${dbSold} -> 漂移 ${drift}`);
        if (dbSold < 0) {
            console.log(`  ❌ 零超卖硬失败: sold_qty 为负(${dbSold})`);
        } else if (dbSold > LIMIT_QTY) {
            console.log(`  ❌ 零超卖硬失败: sold_qty 超限量(${dbSold} > ${LIMIT_QTY})`);
        } else if (drift !== 0) {
            console.log(`  ⚠️ 客户端/服务端计数漂移 ${drift}（可能漏报/多报，需排查）`);
        } else {
            console.log(`  ✅ DB 对账一致，零超卖确认`);
        }
    } else {
        console.log(`  DB 对账: 未执行（teardown 跳过，需 ADMIN_TOKEN）`);
    }
    console.log(`========================================`);

        return {
            'stdout': JSON.stringify({
                success: totalSuccess,
                fail: totalFail,
                sold_out: soldOut,
                user_limit: userLimit,
                total: totalSuccess + totalFail,
                expected_limit: LIMIT_QTY,
                oversold: totalSuccess > LIMIT_QTY,
                list_status: statusCnt,
            }, null, 2),
        };
}


