'use strict';
/**
 * index.js — 瘦编排层
 *
 * 职责：
 *   1. 初始化各模块并注入依赖（接线）
 *   2. 监听事件 → Socket.IO 前端推送（优先级④，setImmediate 后置）
 *   3. 监听事件 → Storage 持久化（优先级⑤，去抖批量）
 *   4. REST API / 静态文件服务
 *   5. 优雅退出
 *
 * 不负责：
 *   - 不负责匹配逻辑（store 内部）
 *   - 不负责买卖决策（trader 内部）
 *   - 不负责数据解码（chain / fourmeme 内部）
 *
 * 优先级分层：
 *   ① 匹配     → store._tryMatch（同步，在 chain/fourmeme 写入时自动触发）
 *   ② 条件判断 → store 内部布尔计算（同步）
 *   ③ 买卖     → trader.onMatched（由 store 同步直调）
 *   ④ 前端推送 → 本文件 setImmediate 包裹的 io.emit
 *   ⑤ 文件写入 → storage.update (2s debounce) / storage.appendEvent (异步)
 */

require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const BlockchainService = require('./blockchain');
const TokenStore = require('./store');
const ChainWatcher = require('./chain');
const FourMemeWatcher = require('./fourmeme');
const TradingEngine = require('./trader');
const Storage = require('./storage');
const { TRADE, NETWORK, WATCH_WALLETS } = require('./config');
const { formatBeijingTimeMs, toReadableTwitter, extractTweetId } = require('./utils');

// ─── 配置组合 ─────────────────────────────────────────────────────────────────
const config = {
  privateKey: TRADE.privateKey,
  bscRpcUrl: NETWORK.bscRpcUrl,
  port: TRADE.port,
  buyAmountBNB: TRADE.buyAmountBNB,
  sellThreshold1USD: TRADE.sellThreshold1USD,
  sellRatio1: TRADE.sellRatio1,
  sellThreshold2USD: TRADE.sellThreshold2USD,
  sellRatio2: TRADE.sellRatio2,
  sellThreshold3USD: TRADE.sellThreshold3USD,
  sellRatio3: TRADE.sellRatio3,
  timeWindowMinutes: TRADE.timeWindowMinutes,
  targetAccounts: TRADE.targetAccounts,
  fixedBNBPrice: TRADE.fixedBNBPrice,
  gasPriceGwei: TRADE.gasPriceGwei,
  buyGasLimit: TRADE.buyGasLimit,
  sellGasLimit: TRADE.sellGasLimit,
  approveGasLimit: TRADE.approveGasLimit,
  // 链上创建事件策略
  createKeywords: TRADE.createKeywords,
  createBuyMinBNB: TRADE.createBuyMinBNB,
  createBuyMaxBNB: TRADE.createBuyMaxBNB,
  createSellThresholdUSD: TRADE.createSellThresholdUSD,
  createSellRatio: TRADE.createSellRatio,
};

// ─── 服务初始化 ───────────────────────────────────────────────────────────────
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });
app.use(express.static(path.join(__dirname, '../public')));

// Storage 最先（供 trader 恢复持仓）
const storage = new Storage();

// BlockchainService（链上交互）
const blockchain = new BlockchainService(config);

// Trader（纯策略引擎）
const trader = new TradingEngine(config, blockchain, storage);

// TokenStore（唯一数据中心，匹配成功同步直调 trader）
const store = new TokenStore({
  targetAccounts: config.targetAccounts,
  timeWindowMinutes: config.timeWindowMinutes,
  bnbPriceUSD: config.fixedBNBPrice,
  onMatched: (token) => trader.onMatched(token), // 优先级③：同步直调（四条件策略）
  // 链上创建事件策略
  createKeywords: config.createKeywords,
  createBuyMinBNB: config.createBuyMinBNB,
  createBuyMaxBNB: config.createBuyMaxBNB,
  onCreateMatched: (token) => trader.onCreateMatched(token), // 优先级③：同步直调（创建策略）
});

// ChainWatcher（BSC 单WSS）
const chain = new ChainWatcher();
chain.store = store;

// FourMemeWatcher（独立WSS）
const fourmeme = new FourMemeWatcher();
fourmeme.store = store;

// ─── 恢复持久化的 MEME 数据 ──────────────────────────────────────────────────
const restoredMemes = storage.loadAllMemes();
if (restoredMemes.length > 0) {
  let restored = 0;
  for (const meme of restoredMemes) {
    if (!meme.tokenId || store.has(meme.tokenId)) continue;
    const ca = meme.tokenId.toLowerCase ? meme.tokenId.toLowerCase() : meme.tokenId;
    store.tokenMap.set(ca, {
      ...meme,
      tokenId: ca,
      tokenAddress: meme.tokenAddress || ca,
      arrivalTime: meme.arrivalTime ? new Date(meme.arrivalTime) : new Date(),
      walletSignals: meme.walletSignals || [],
      trades: meme.trades || [],
      _enriched: true,
    });
    restored++;
  }
  console.log(`[Server] ♻️ 恢复 ${restored} 个持久化MEME到内存`);
}

// ─── 运行时状态 ───────────────────────────────────────────────────────────────
let bnbPriceUSD = config.fixedBNBPrice;
let wsChainStatus = false;
let wsFourmemeStatus = false;
let walletAddress = null;
let bnbBalance = '—';

// walletTxHistory 从 Storage 恢复
const walletTxHistory = (storage.state.walletTxHistory || []).slice(0, 500);
console.log(`[Server] ♻️ 恢复 walletTxHistory: ${walletTxHistory.length} 条`);

// ─── 辅助 ─────────────────────────────────────────────────────────────────────
function syncBNBPrice(price) {
  if (price > 0) {
    bnbPriceUSD = price;
    store.setBNBPrice(price);
    trader.setBNBPrice(price);
    io.emit('bnb_price', { price: bnbPriceUSD });
  }
}

async function updateWalletBalance() {
  try {
    const bal = await blockchain.getBNBBalance();
    bnbBalance = bal || '0';
    walletAddress = blockchain.getWalletAddress();
    io.emit('wallet_balance', { address: walletAddress, balance: bnbBalance });
  } catch (_) { bnbBalance = '—'; }
}

// ─── four.meme API 轮询竞速（与 WSS TOKEN_EVENT 竞速）─────────────────────────
// TOKEN_CREATE 到达后立即启动，每500ms请求一次，最多4次
// 有 data 返回就停止 → store.enrich → 记录来源+耗时

const API_POLL_MAX_RETRIES = 3;
const API_POLL_DELAYS = [800, 1000, 1200]; // 并发竞速：800ms/1000ms/1200ms 三次同时发出
const FOUR_MEME_API_URL = NETWORK.fourMemeApi;

// UA 池：模拟不同浏览器/平台，防止被限流
const UA_POOL = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15',
  'Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Edg/125.0.0.0',
  'Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1',
];

function getRandomHeaders() {
  return {
    'User-Agent': UA_POOL[Math.floor(Math.random() * UA_POOL.length)],
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': ['en-US,en;q=0.9', 'zh-CN,zh;q=0.9,en;q=0.8', 'ja,en;q=0.9'][Math.floor(Math.random() * 3)],
    'Referer': 'https://four.meme/',
    'Origin': 'https://four.meme',
    'sec-ch-ua-platform': ['"Windows"', '"macOS"', '"Linux"'][Math.floor(Math.random() * 3)],
  };
}

// 正在轮询的 CA → AbortController（WSS 先到时中止）
const apiPollingMap = new Map();

/**
 * 主动轮询 four.meme API 获取代币元数据（并发竞速）
 * 800ms/1000ms/1200ms 三次并发，先到者赢
 * @param {string} ca - 代币合约地址（lowercase）
 */
async function pollFourMemeApi(ca) {
  if (!ca || !FOUR_MEME_API_URL) return;

  const controller = new AbortController();
  apiPollingMap.set(ca, controller);

  const url = `${FOUR_MEME_API_URL}${ca}`;
  let resolved = false;

  const tryFetch = async (delay, attempt) => {
    await new Promise(r => setTimeout(r, delay));

    // 已被 WSS 先到或其他并发请求先完成
    if (resolved || controller.signal.aborted) return;

    const token = store.get(ca);
    if (!token || token._enriched) { resolved = true; return; }

    try {
      const res = await fetch(url, {
        method: 'GET',
        headers: getRandomHeaders(),
        signal: controller.signal,
      });
      const json = await res.json();

      if (resolved || controller.signal.aborted) return;

      const data = json?.data;
      if (!data || typeof data !== 'object' || Object.keys(data).length === 0) return;

      // 再次检查 WSS 是否先到
      if (token._enriched) { resolved = true; return; }

      // 竞速胜出：标记并处理
      resolved = true;

      const apiLatency = Date.now() - token.arrivalTime.getTime();

      const rawTwitter = data.twitterUrl || data.twitter || data.webUrl || null;
      const rawImage = data.image || data.img || data.logoUrl || null;
      const rawSymbol = data.symbol || data.ticker || null;
      const rawName = data.name || data.tokenName || null;
      const rawTamount = data.tamount || data.totalSupply || null;

      const enrichData = {
        image: rawImage || null,
        fmSymbol: rawSymbol || null,
        fmName: rawName || null,
        tamount: rawTamount || null,
        mediaAddressTime: formatBeijingTimeMs(new Date()),
        _source: 'api',
      };

      if (rawTwitter) {
        const readable = toReadableTwitter(rawTwitter);
        enrichData.twitterUrl = rawTwitter;
        enrichData.twitterDisplay = readable.display;
        enrichData.twitterHref = readable.href;
        enrichData.twitterUsername = readable.username;
      }

      store.enrich(ca, enrichData);

      io.emit('media_race', { ca, latency: apiLatency, source: 'api', channel: 'API', attempt });
      console.log(`[API轮询] 🚀 API胜出 | CA:${ca.slice(0, 10)}... | 第${attempt}次 | ${apiLatency}ms`);

      if (rawTwitter) {
        const tweetId = extractTweetId(rawTwitter);
        const readable = toReadableTwitter(rawTwitter);
        const isAnonymous = !!(rawTwitter && /\/(i)\/status\//i.test(rawTwitter) && !readable.username);
        if (isAnonymous && tweetId) {
          fourmeme._resolveAnonymousUrl(ca, tweetId);
        } else if (tweetId) {
          fourmeme._asyncFetchContent(ca, tweetId);
        }
        fourmeme._apiResolved.add(ca);
      }
    } catch (err) {
      if (err.name === 'AbortError') return;
      console.warn(`[API轮询] ⚠️ 第${attempt}次失败: ${err.message} | CA:${ca.slice(0, 10)}...`);
    }
  };

  // 三次并发竞速
  await Promise.allSettled(
    API_POLL_DELAYS.map((delay, i) => tryFetch(delay, i + 1))
  );

  if (!resolved) {
    console.log(`[API轮询] ❌ 3次均无数据 | CA:${ca.slice(0, 10)}... | 等待WSS兜底`);
  }
  apiPollingMap.delete(ca);
}

// ─── 事件监听：优先级④ 前端推送 ───────────────────────────────────────────────

// Store 事件 → 前端
store.on('registered', (token) => {
  io.emit('new_token', token);
  io.emit('token_count', { count: store.size });
  // 通知 fourmeme 回放缓冲（WSS 早到事件）
  fourmeme.registerCA(token.tokenId);
  // 立即启动 API 轮询（与 WSS 竞速）
  pollFourMemeApi(token.tokenId);
  // 非狙击MEME不持久化
});

store.on('enriched', (token) => {
  // 如果 WSS 先到了，中止正在进行的 API 轮询
  if (token._enriched && apiPollingMap.has(token.tokenId)) {
    const ctrl = apiPollingMap.get(token.tokenId);
    ctrl.abort();
    apiPollingMap.delete(token.tokenId);
  }

  io.emit('token_enriched', {
    tokenId: token.tokenId,
    image: token.image,
    fmSymbol: token.fmSymbol,
    fmName: token.fmName,
    twitterUrl: token.twitterUrl,
    twitterDisplay: token.twitterDisplay,
    twitterHref: token.twitterHref,
    twitterUsername: token.twitterUsername,
    mediaAddressTime: token.mediaAddressTime,
    twitterCreatedAt: token.twitterCreatedAt,
    twitterContent: token.twitterContent,
    twitterContentTime: token.twitterContentTime,
    tamount: token.tamount,
    mediaSource: token.mediaSource,
    mediaLatencyMs: token.mediaLatencyMs,
  });

  // 只有狙击MEME才持久化
  if (token.bought || token.matchReason || token._createStrategyMatched) {
    setImmediate(() => storage.persistMeme(token));
  }
});

store.on('price_updated', ({ token, priceBNB, marketCapUSD }) => {
  io.emit('price_update', {
    tokenId: token.tokenId,
    marketCapUSD,
    price: priceBNB,
    source: 'chain_trade',
  });

  // 止盈检查（仅持仓代币）
  if (trader.hasPosition(token.tokenAddress)) {
    trader.onPriceUpdate(token.tokenAddress, marketCapUSD, token).catch(() => {});
  }
});

store.on('matched', (token) => {
  io.emit('matched_token', token);
  // 优先级⑤：持久化匹配事件
  setImmediate(() => {
    storage.appendEvent('matched_token', {
      tokenId: token.tokenId,
      symbol: token.symbol,
      matchReason: token.matchReason,
      marketCapUSD: token.marketCapUSD,
    });
  });
});

store.on('create_matched', (token) => {
  io.emit('create_matched_token', token);
  // 优先级⑤：持久化链上创建策略匹配事件
  setImmediate(() => {
    storage.appendEvent('create_matched_token', {
      tokenId: token.tokenId,
      symbol: token.symbol,
      matchReason: token._createMatchReason,
      creatorBuyBNB: token._creatorBuyBNB,
      keyword: token._createKeyword,
    });
  });
});

store.on('wallet_signal', ({ token, signal }) => {
  io.emit('token_signal', {
    tokenId: token.tokenId,
    time: signal.time,
    walletName: signal.walletName,
    walletAddress: signal.walletAddress,
    bnbAmount: signal.bnbAmount,
    tokenAmount: signal.tokenAmount,
    txHash: signal.txHash,
    marketCapUSD: signal.marketCapUSD || token.marketCapUSD || 0,
  });
});

// Chain 事件 → 前端（交易明细统一通过 store 的 trade_added 事件推送）
store.on('trade_added', ({ token, trade }) => {
  io.emit('fm_trade', {
    source: trade.source || 'chain',
    tokenAddress: trade.tokenAddress,
    userAddress: trade.userAddress,
    tokenName: token.name || trade.tokenName || trade.tokenAddress,
    volume: trade.bnbAmount,
    side: trade.side === 'buy' ? 1 : 2,
    sideLabel: trade.sideLabel,
    image: token.image || trade.tokenImage || '',
    symbol: 'BNB',
    txHash: trade.txHash,
    blockNumber: trade.blockNumber,
    time: trade.time,
    tokenSymbol: token.symbol || token.fmSymbol || trade.tokenSymbol || '',
    tokenTwitterUrl: token.twitterUrl || trade.tokenTwitterUrl || '',
    tokenTwitterDisp: token.twitterDisplay || trade.tokenTwitterDisp || '',
    tokenTwitterHref: token.twitterHref || trade.tokenTwitterHref || '',
    marketCapUSD: token.marketCapUSD || 0,
  });

  // 只持久化狙击MEME的交易数据
  if (token.bought || token.matchReason || token._createStrategyMatched) {
    setImmediate(() => storage.persistTrade(token));
  }
});

chain.on('wallet_trade', (record) => {
  // 保存历史
  walletTxHistory.unshift(record);
  if (walletTxHistory.length > 500) walletTxHistory.length = 500;

  io.emit('wallet_tx', record);

  // 优先级⑤：持久化
  setImmediate(() => {
    storage.update({ walletTxHistory: walletTxHistory.slice(0, 500) });
    storage.appendEvent('wallet_trade', {
      time: record.time,
      walletName: record.walletName,
      action: record.action,
      tokenAddress: record.tokenAddress,
      tokenSymbol: record.tokenSymbol,
      bnbAmount: record.bnbAmount,
      txHash: record.txHash,
      marketCapUSD: record.marketCapUSD,
    });
  });
});


// 连接状态
chain.on('connected', () => {
  wsChainStatus = true;
  io.emit('ws_status', { chain: true, fourmeme: null, dex: null, status: '链上已连接' });
});
chain.on('disconnected', () => {
  wsChainStatus = false;
  io.emit('ws_status', { chain: false, fourmeme: null, dex: null, status: '链上断开重连中...' });
});
fourmeme.on('connected', () => {
  wsFourmemeStatus = true;
  io.emit('ws_status', { chain: null, fourmeme: true, dex: null, status: '' });
});
fourmeme.on('disconnected', () => {
  wsFourmemeStatus = false;
  io.emit('ws_status', { chain: null, fourmeme: false, dex: null, status: '' });
});

// FourMeme WSS 通道竞速时间差 → 前端显示
fourmeme.on('wss_enriched', (data) => {
  io.emit('media_race', { ...data, channel: 'WSS' });
});

// FourMeme TICKER_EVENT → BNB 实时价格（唯一价格源）
fourmeme.on('bnb_price', (price) => {
  syncBNBPrice(price);
});

// Trader 事件 → 前端 + 持久化
trader.on('trade', (trade) => {
  io.emit('trade', trade);
  io.emit('trade_history', trader.getTradeHistory());
  setImmediate(() => {
    storage.appendEvent('trade', trade);
    updateWalletBalance();
  });
});
trader.on('trade_update', (patch) => { io.emit('trade_update', patch); });

// ─── Socket.IO 连接（初始化数据） ─────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log('[Server] 前端已连接:', socket.id);
  socket.emit('init', {
    tokens: store.getAllTokens(),  // Send all tokens, frontend handles pagination
    tradeHistory: trader.getTradeHistory(),
    positions: trader.getPositions(),
    bnbPrice: bnbPriceUSD,
    wsStatus: wsChainStatus ? '链上已连接' : '未连接',
    wsChain: wsChainStatus,
    wsFourmeme: wsFourmemeStatus,
    walletAddress,
    bnbBalance,
    watchWallets: WATCH_WALLETS,
    walletTxHistory: walletTxHistory.slice(0, 200),
    nodeStatus: null,
    config: {
      buyAmountBNB: config.buyAmountBNB,
      sellThreshold1USD: config.sellThreshold1USD,
      sellThreshold2USD: config.sellThreshold2USD,
      sellThreshold3USD: config.sellThreshold3USD,
      sellRatio1: config.sellRatio1,
      sellRatio2: config.sellRatio2,
      sellRatio3: config.sellRatio3,
      targetAccounts: config.targetAccounts,
      timeWindowMinutes: config.timeWindowMinutes,
    },
  });
});

// ─── REST API ─────────────────────────────────────────────────────────────────
app.get('/api/status', (_req, res) => {
  res.json({
    wsStatus: wsChainStatus ? '链上已连接' : '未连接',
    walletAddress,
    bnbBalance,
    bnbPrice: bnbPriceUSD,
    tokenCount: store.size,
    positions: trader.getPositions().length,
    watchWallets: WATCH_WALLETS,
  });
});
app.get('/api/tokens', (_req, res) => {
  const page = parseInt(_req.query.page) || 1;
  const limit = parseInt(_req.query.limit) || 20;
  const tokens = store.getAllTokens();
  const total = tokens.length;
  const totalPages = Math.ceil(total / limit);
  const start = (page - 1) * limit;
  const end = Math.min(start + limit, total);
  res.json({
    tokens: tokens.slice(start, end),
    page,
    totalPages,
    total,
  });
});
app.get('/api/trades', (_req, res) => res.json(trader.getTradeHistory()));
app.get('/api/positions', (_req, res) => res.json(trader.getPositions()));
app.get('/api/wallet-txs', (_req, res) => res.json(walletTxHistory.slice(0, 200)));

// ─── 优雅退出 ─────────────────────────────────────────────────────────────────
function gracefulShutdown(signal) {
  console.log(`\n[Server] 收到 ${signal}，安全退出...`);
  chain.stop();
  fourmeme.stop();
  // 持久化所有狙击MEME（同步写入）
  const allTokens = store.getAllTokens().filter(t => t.bought || t.matchReason || t._createStrategyMatched);
  for (const token of allTokens) {
    const ca = (token.tokenId || '').toLowerCase();
    if (!ca) continue;
    const filePath = path.join(__dirname, '..', 'data', 'memes', `${ca}.json`);
    try {
      const payload = JSON.stringify({
        meta: {
          tokenId: token.tokenId, tokenAddress: token.tokenAddress,
          symbol: token.symbol, name: token.name, fmSymbol: token.fmSymbol, fmName: token.fmName,
          image: token.image, twitterUrl: token.twitterUrl, twitterDisplay: token.twitterDisplay,
          twitterHref: token.twitterHref, twitterUsername: token.twitterUsername,
          twitterCreatedAt: token.twitterCreatedAt, twitterContent: token.twitterContent,
          programGetTime: token.programGetTime, mediaAddressTime: token.mediaAddressTime,
          twitterContentTime: token.twitterContentTime, marketCapUSD: token.marketCapUSD,
          mediaSource: token.mediaSource, mediaLatencyMs: token.mediaLatencyMs,
          mediaMatched: token.mediaMatched, mediaTimeMatched: token.mediaTimeMatched,
          walletSignals: (token.walletSignals || []).slice(0, 100),
          bought: token.bought, sold1: token.sold1, sold2: token.sold2,
          matchReason: token.matchReason, buyStatus: token.buyStatus,
          arrivalTime: token.arrivalTime, tamount: token.tamount,
        },
        trades: (token.trades || []).slice(-200).map(t => ({
          side: t.side, userAddress: t.userAddress, bnbAmount: t.bnbAmount,
          tokenAmount: t.tokenAmount, txHash: t.txHash, time: t.time, marketCapUSD: t.marketCapUSD || 0,
        })),
      });
      fs.writeFileSync(filePath, payload);
    } catch (_) {}
  }
  store.destroy();
  storage.flushSync();
  console.log(`[Server] 已持久化 ${allTokens.length} 个MEME，退出。`);
  process.exit(0);
}
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('uncaughtException', (err) => {
  console.error('[Server] 未捕获异常:', err);
  storage.flushSync();
});

// ─── 启动 ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log('');
  console.log('╔══════════════════════════════════════════════════╗');
  console.log('║  BitSticker — 优先级驱动自动交易机器人            ║');
  console.log('║  3条BSC WSS | 独立FourMeme WSS | 双策略瞬间买入  ║');
  console.log('╚══════════════════════════════════════════════════╝');
  console.log('');

  const chainOk = await blockchain.init();
  walletAddress = blockchain.getWalletAddress();
  if (chainOk) await updateWalletBalance();

  console.log(`[Price] BNB 等待 FourMeme WSS TICKER_EVENT 实时推送...`);

  // 定时刷新余额
  setInterval(updateWalletBalance, 60_000);

  // 定时持久化所有狙击MEME（每5分钟）
  setInterval(() => {
    const tokens = store.getAllTokens().filter(t => t.bought || t.matchReason || t._createStrategyMatched);
    for (const t of tokens) storage.persistMeme(t);
    if (tokens.length > 0) console.log(`[Storage] 📦 定时持久化 ${tokens.length} 个狙击MEME`);
  }, 5 * 60_000);

  // 启动双 WSS（完全独立，互不影响）
  chain.start();
  fourmeme.start();

  server.listen(config.port, () => {
    console.log(`[Server] 🌐 http://localhost:${config.port}`);
    console.log(`[Server] 买入: ${config.buyAmountBNB} BNB | GAS: ${config.gasPriceGwei} Gwei`);
    console.log(`[Server] 止盈(四条件): $${(config.sellThreshold1USD / 1000).toFixed(0)}K→${config.sellRatio1 * 100}% | $${(config.sellThreshold2USD / 1000).toFixed(0)}K→${config.sellRatio2 * 100}% | $${(config.sellThreshold3USD / 1000).toFixed(0)}K→${config.sellRatio3 * 100}%`);
    console.log(`[Server] 止盈(创建策略): $${(config.createSellThresholdUSD / 1000).toFixed(0)}K→${config.createSellRatio * 100}% 全卖`);
    console.log(`[Server] 创建策略关键词: ${config.createKeywords.length} 个 | 创建者买入: ${config.createBuyMinBNB}~${config.createBuyMaxBNB} BNB`);
    console.log(`[Server] 目标: ${config.targetAccounts.join(', ')}`);
    console.log(`[Server] 钱包: ${WATCH_WALLETS.length} 个`);
    console.log('');
  });
}

main().catch(err => {
  console.error('启动失败:', err);
  storage.flushSync();
  process.exit(1);
});
