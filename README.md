# BitSticker — 优先级驱动 Four.meme 自动交易机器人

BSC 链上 MEME 代币自动发现、匹配、买入、止盈系统。

---

## 架构概览

```
┌─────────── WSS-1: TOKEN_CREATE + 钱包监控 (publicnode) ────────────┐
│  subscribe#A: {address: FACTORY, topics: [TOKEN_CREATE_TOPIC]}      │
│    └─ TOKEN_CREATE → store.register (新币发现)                      │
│  subscribe#B: {topics: [Transfer, [wallets]]}  ← 钱包卖出(补漏)    │
│  subscribe#C: {topics: [Transfer, null, [wallets]]} ← 钱包买入     │
└─────────────────────────────────────────────────────────────────────┘

┌─────────── WSS-2: TOKEN_BUY (drpc) ───────────────────────────────┐
│  subscribe: {address: FACTORY, topics: [TOKEN_BUY_TOPIC]}          │
│    └─ TOKEN_BUY → store.updatePrice + store.addWalletSignal        │
└─────────────────────────────────────────────────────────────────────┘

┌─────────── WSS-3: TOKEN_SELL (nodereal) ──────────────────────────┐
│  subscribe: {address: FACTORY, topics: [TOKEN_SELL_TOPIC]}         │
│    └─ TOKEN_SELL → store.updatePrice (市值更新)                    │
└─────────────────────────────────────────────────────────────────────┘

┌─────────── WSS-4: FourMeme (完全独立) ────────────────────────────┐
│  @TOKEN_EVENT@0 → store.enrich (image, twitterUrl, symbol)         │
│  匿名URL → oEmbed(~500ms) ∥ socialdata(~4s) 并发竞速              │
└─────────────────────────────────────────────────────────────────────┘

         ↓ 所有数据写入（CA MAP 匹配）
┌─────────── TokenStore (唯一数据中心) ──────────────────────────────┐
│  tokenMap: Map<CA, Token>  ← CA 为唯一索引                         │
│  每次写入 → _tryMatch(token) → 四条件全满足 → trader.onMatched()   │
└─────────────────────────────────────────────────────────────────────┘

         ↓ matched (同步直调)
┌─────────── TradingEngine ─────────────────────────────────────────┐
│  买入: chain.buyToken(ca, 0.01 BNB) → 立即返回hash(不等确认)      │
│  卖出: MC≥30K→50% | MC≥40K→50% | MC≥100K→100%(清仓)              │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3 WSS 独立供应商架构

### 设计动机

单 WSS 连接订阅工厂全部事件（CREATE + BUY + SELL），在交易量大时产生数据洪流，
导致单供应商配额耗尽 → 限速 → 断连 → 丢失关键事件。

### 解决方案

将 3 种事件分流到 3 条独立 WSS 连接，**各用不同 RPC 供应商**，彻底消除共享配额：

| 通道 | 事件类型 | 流量级别 | 供应商 | 端点 |
|------|---------|----------|--------|------|
| WSS-1 | TOKEN_CREATE + Transfer(钱包监控) | 低 | publicnode | `wss://bsc-rpc.publicnode.com` |
| WSS-2 | TOKEN_BUY | **高** | drpc | `wss://bsc.drpc.org` |
| WSS-3 | TOKEN_SELL | **高** | nodereal | `wss://bsc.nodereal.io/ws/v1/...` |

### 核心特性

- **独立配额**：3 个供应商各自独立的请求配额，单通道限速不影响其他
- **独立重连**：每条 WSS 各自管理连接生命周期，断线后 1s 自动重连
- **独立容错**：WSS-3 断了不影响 WSS-1 发现新币 + WSS-2 钱包买入信号
- **状态聚合**：任一通道连接成功即视为系统可用，全部断开才 emit disconnected
- **零代码侵入**：对外事件接口不变，store / index / trader 无需修改

### WssChannel 复用设计

```js
class WssChannel {
  constructor(name, url, subscriptions, onLog, onStatus) { ... }
  // 轻量连接管理：start/stop/reconnect
  // 所有通道共享同一个 _dispatch 分发函数
}
```

---

## 优先级原则

| 优先级 | 职责 | 实现方式 |
|--------|------|----------|
| ① 匹配 | 四条件评估 | store 写入方法末尾同步调用 `_tryMatch` |
| ② 条件判断 | 布尔值读取 | 纯同步，~0.01ms |
| ③ 瞬间买卖 | 发送交易 | `_onMatched` 同步直调 trader，不经事件循环 |
| ④ 前端推送 | Socket.IO | `setImmediate` 后置，永不阻塞主路径 |
| ⑤ 文件写入 | state.json | 2s debounce 批量写，异步追加 events.ndjson |

---

## 四条件匹配

所有条件以 **CA 为唯一关联**，任何一条数据到达都可能是"最后一块拼图"：

| # | 条件 | 数据来源 | 触发写入方法 |
|---|------|----------|-------------|
| ① | 媒体链接命中 targetAccounts | FourMeme WSS | `store.enrich()` |
| ② | 媒体创建时间与代币创建时间差 < 3分钟 | FourMeme WSS (tweet ID 推算) | `store.enrich()` |
| ③ | 监控钱包对该CA有买入记录 | Chain WSS-2 (TOKEN_BUY / Transfer) | `store.addWalletSignal()` |
| ④ | 市值 < 10K USD | Chain WSS-2/3 (TOKEN_BUY/SELL 价格计算) | `store.updatePrice()` |

---

## 文件结构

```
src/
├── index.js        # 瘦编排层：接线 + 前端推送 + REST + 退出
├── config.js       # 统一配置：3条WSS端点/钱包/交易参数
├── chain.js        # 3条独立BSC WSS：WssChannel复用 + 统一_dispatch
├── fourmeme.js     # FourMeme WSS：TOKEN_EVENT 媒体补充
├── store.js        # TokenStore：唯一数据中心 + 四条件匹配引擎
├── trader.js       # TradingEngine：纯买卖策略
├── blockchain.js   # BlockchainService：链上交互（nonce管理/发tx）
├── storage.js      # 持久化：state.json + events.ndjson
└── utils.js        # 公共工具函数
```

---

## 改造对比

| 维度 | 改造前 | 改造后 | 改善 |
|------|--------|--------|------|
| BSC WSS 连接数 | 1条（全部事件共享） | **3条**（按事件分流） | 限速风险降为 1/3 |
| 供应商配额 | 单供应商扛全部流量 | 3家独立配额 | 彻底消除限速 |
| 断连影响 | 全部事件丢失 | 仅对应事件类型受影响 | 容错提升 3× |
| 连接管理代码 | 单一庞大 _connect | WssChannel 轻量类复用 | 代码更简洁 |
| topic 过滤 | `address: FACTORY`（全量推送） | 精确 topic 过滤 | 节省 RPC 带宽 |
| 匹配→买入延迟 | ~2ms | **~2ms（不变）** | 架构不增加延迟 |
| store/trader | 不变 | 不变 | 零侵入 |

---

## 4 条 WSS 独立性保障

```
WSS-1 (Create)   WSS-2 (Buy)     WSS-3 (Sell)    WSS-4 (FourMeme)
     │                │                │                │
     │ 各自独立       │ 各自独立       │ 各自独立       │ 各自独立
     │ 各自重连       │ 各自重连       │ 各自重连       │ 各自重连
     │                │                │                │
     │ 断线时：       │ 断线时：       │ 断线时：       │ 断线时：
     │ 丢失新币发现   │ 丢失买入信号   │ 丢失卖出价格   │ 丢失媒体数据
     │ (不影响买卖)   │ (条件③暂停)   │ (市值更新暂停) │ (不影响买卖)
     │                │                │                │
     └───── 重连后各自恢复，全部通过 CA MAP 合并到 store ─────────────┘
```

---

## 环境变量

```bash
# 3 条 BSC WSS（可按需替换供应商）
BSC_WSS_CREATE=wss://bsc-rpc.publicnode.com
BSC_WSS_BUY=wss://bsc.drpc.org
BSC_WSS_SELL=wss://bsc.nodereal.io/ws/v1/64a9df0874fb4a93b9d0a3849de012d3

# HTTP RPC（发交易/查余额）
BSC_RPC_URL=https://bsc-dataseed1.binance.org

# 其他配置见 src/config.js
```

---

## 运行

```bash
npm install
node src/index.js
```

---

## 数据流时间线示例

```
T+0ms      WSS-1: TOKEN_CREATE → store.register(ca)
T+200ms    WSS-4: TOKEN_EVENT → store.enrich(ca, {twitterUrl, image})
             内部: ① mediaMatched=true ② mediaTimeMatched=true
             → _tryMatch: ①✅ ②✅ ③✗ ④✗ → 不触发
T+800ms    WSS-2: TOKEN_BUY(路人) → store.updatePrice(ca)
             → MC=$3200, ④ 满足
             → _tryMatch: ①✅ ②✅ ③✗ ④✅ → 不触发
T+1200ms   WSS-2: TOKEN_BUY(监控钱包) → store.updatePrice + addWalletSignal
             → ③ 满足
             → _tryMatch: ①✅ ②✅ ③✅ ④✅ → 🎯 匹配！
             → trader.onMatched → buyToken → tx hash 返回
T+1203ms   买入完成（不等确认）
T+1204ms   setImmediate → io.emit('matched_token') → 前端显示
T+3200ms   storage debounce → state.json 写入
```
