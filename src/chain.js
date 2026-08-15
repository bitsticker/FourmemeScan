'use strict';
/**
 * chain.js — BSC 链上事件监听（重构版）
 *
 * 架构（第一性原理 MVC：数据获取唯一入口，多处同步更新）：
 *
 *   WSS-1 (TOKEN_CREATE专用)：
 *     - 监听 TOKEN_CREATE 事件
 *     - 配置 3 条备用 RPC，任一断开自动切换到下一条（循环轮换）
 *     - 所有3条RPC订阅完全相同的过滤条件
 *     - _seenCA 去重，避免同一创建事件被多条RPC重复处理
 *
 *   WSS-2 (TOKEN_BUY + TOKEN_SELL)：
 *     - 单连接监听全部买卖事件
 *     - 断线自动重连
 *     - 同时监听钱包 Transfer 补漏
 *
 *   WSS-3：已删除
 *   WSS-4：four.meme（独立模块 fourmeme.js，保持不变）
 *
 * 优先级：数据获取 → 策略匹配 → 交易执行 → 前端推送 → 持久化
 */

const WebSocket = require('ws');
const { ethers } = require('ethers');
const { EventEmitter } = require('events');
const { formatBeijingTimeMs } = require('./utils');
const { NETWORK, WATCH_WALLETS } = require('./config');

// ── 常量 ─────────────────────────────────────────────────────────────────────
const FACTORY_ADDRESS = '0x5c952063c7fc8610ffdb798152d69f0b9550762b';
const TOKEN_CREATE_TOPIC = '0x396d5e902b675b032348d3d2e9517ee8f0c4a926603fbc075d3d282ff00cad20';
const TOKEN_BUY_TOPIC    = '0x7db52723a3b2cdd6164364b3b766e65e540d7be48ffa89582956d8eaebe62942';
const TOKEN_SELL_TOPIC   = '0x0a5575b3648bae2210cee56bf33254cc1ddfbc7bf637c0af2ac18b14fb1bae19';
const ERC20_TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
const WBNB_ADDRESS = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c';
const FACTORY_LOWER = FACTORY_ADDRESS.toLowerCase();
const ZERO_ADDR = '0x0000000000000000000000000000000000000000';

const CREATE_TYPES = ['address', 'address', 'uint256', 'string', 'string', 'uint256'];
const TRADE_TYPES  = ['address', 'address', 'uint256', 'uint256', 'uint256', 'uint256', 'uint256', 'uint256'];

// ── WSS-1：TOKEN_CREATE 专用，3条RPC自动轮换 ─────────────────────────────────
/**
 * RotatingWssChannel — 带RPC池的WSS通道
 * 断线后自动切换到池中的下一条RPC（循环），不断重试直到连接成功
 */
class RotatingWssChannel {
  /**
   * @param {string}   name         通道名称（日志用）
   * @param {string[]} urlPool      RPC URL 池（按优先级排列）
   * @param {object[]} subscriptions 订阅参数列表
   * @param {function} onLog        收到 log 的回调
   * @param {function} onStatus     连接状态变化回调 (connected: boolean)
   */
  constructor(name, urlPool, subscriptions, onLog, onStatus) {
    this.name = name;
    this.urlPool = urlPool.filter(Boolean);
    this.subscriptions = subscriptions;
    this.onLog = onLog;
    this.onStatus = onStatus || (() => {});
    this.running = false;
    this._ws = null;
    this._rpcId = 1;
    this._curIdx = 0;  // 当前使用的RPC下标
    this._reconnecting = false;
  }

  start() { this.running = true; this._connect(); }
  stop()  { this.running = false; this._destroy(); }

  get connected() {
    return !!(this._ws && this._ws.readyState === WebSocket.OPEN);
  }

  _currentUrl() {
    return this.urlPool[this._curIdx % this.urlPool.length];
  }

  _nextUrl() {
    this._curIdx = (this._curIdx + 1) % this.urlPool.length;
    return this._currentUrl();
  }

  _destroy() {
    try { if (this._ws) this._ws.terminate(); } catch (_) {}
    this._ws = null;
  }

  _connect() {
    if (!this.running) return;
    this._reconnecting = false;
    this._destroy();

    const url = this._currentUrl();
    let ws;
    try { ws = new WebSocket(url); } catch (err) {
      console.error(`[${this.name}] WS创建失败 (${url}):`, err.message);
      this._scheduleReconnect(true);
      return;
    }
    this._ws = ws;

    ws.on('open', () => {
      if (!this.running) { ws.terminate(); return; }
      console.log(`[${this.name}] ✅ 已连接 ${url} (RPC ${this._curIdx + 1}/${this.urlPool.length})`);
      this.onStatus(true);
      for (const sub of this.subscriptions) {
        this._send({ method: 'eth_subscribe', params: ['logs', sub] });
      }
    });

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch (_) { return; }
      if (msg.id && msg.result && typeof msg.result === 'string') return; // sub确认
      if (msg.method === 'eth_subscription' && msg.params && msg.params.result) {
        this.onLog(msg.params.result);
      }
    });

    ws.on('close', (code) => {
      console.warn(`[${this.name}] 断开 (code=${code}) | 切换下一条RPC...`);
      this._ws = null;
      this.onStatus(false);
      // 断线时切换到下一条RPC
      this._nextUrl();
      this._scheduleReconnect(false);
    });

    ws.on('error', (err) => {
      console.error(`[${this.name}] 错误:`, err.message);
    });
  }

  _send(payload) {
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify({ jsonrpc: '2.0', id: this._rpcId++, ...payload }));
    }
  }

  _scheduleReconnect(sameUrl = false) {
    if (!this.running || this._reconnecting) return;
    this._reconnecting = true;
    // 切换到不同RPC时稍微延迟，同一URL重试时延迟更长
    const delay = sameUrl ? 3000 : 1000;
    setTimeout(() => { if (this.running) this._connect(); }, delay);
  }
}

// ── 标准单连接WSS通道（WSS-2复用） ───────────────────────────────────────────
class WssChannel {
  constructor(name, url, subscriptions, onLog, onStatus) {
    this.name = name;
    this.url = url;
    this.subscriptions = subscriptions;
    this.onLog = onLog;
    this.onStatus = onStatus || (() => {});
    this.running = false;
    this._ws = null;
    this._rpcId = 1;
    this._reconnecting = false;
  }

  start() { this.running = true; this._connect(); }
  stop()  { this.running = false; this._destroy(); }

  get connected() {
    return !!(this._ws && this._ws.readyState === WebSocket.OPEN);
  }

  _destroy() {
    try { if (this._ws) this._ws.terminate(); } catch (_) {}
    this._ws = null;
  }

  _connect() {
    if (!this.running) return;
    this._reconnecting = false;
    this._destroy();

    let ws;
    try { ws = new WebSocket(this.url); } catch (err) {
      console.error(`[${this.name}] WS创建失败:`, err.message);
      this._scheduleReconnect();
      return;
    }
    this._ws = ws;

    ws.on('open', () => {
      if (!this.running) { ws.terminate(); return; }
      console.log(`[${this.name}] ✅ 已连接 ${this.url}`);
      this.onStatus(true);
      for (const sub of this.subscriptions) {
        this._send({ method: 'eth_subscribe', params: ['logs', sub] });
      }
    });

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch (_) { return; }
      if (msg.id && msg.result && typeof msg.result === 'string') return;
      if (msg.method === 'eth_subscription' && msg.params && msg.params.result) {
        this.onLog(msg.params.result);
      }
    });

    ws.on('close', (code) => {
      console.warn(`[${this.name}] 断开 (code=${code})，1s后重连...`);
      this._ws = null;
      this.onStatus(false);
      this._scheduleReconnect();
    });

    ws.on('error', (err) => {
      console.error(`[${this.name}] 错误:`, err.message);
    });
  }

  _send(payload) {
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify({ jsonrpc: '2.0', id: this._rpcId++, ...payload }));
    }
  }

  _scheduleReconnect() {
    if (!this.running || this._reconnecting) return;
    this._reconnecting = true;
    setTimeout(() => { if (this.running) this._connect(); }, 1000);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  ChainWatcher — 管理 WSS-1（CREATE，3条RPC轮换）+ WSS-2（BUY+SELL）
// ═══════════════════════════════════════════════════════════════════════════════

class ChainWatcher extends EventEmitter {
  constructor() {
    super();
    this.running = false;
    this._abiCoder = new ethers.AbiCoder();
    this._seenCA = new Set();  // CREATE 去重（多条RPC同时订阅，只处理一次）

    // 监控钱包 Map: address(lowercase) → name
    this.watchWallets = new Map(
      WATCH_WALLETS.map(w => [w.address.toLowerCase(), w.name])
    );

    // Transfer 事件的钱包地址 topic 格式
    this._walletTopics = Array.from(this.watchWallets.keys()).map(
      addr => '0x000000000000000000000000' + addr.slice(2)
    );

    // Transfer 去重
    this._recentTxs = new Map();
    this._DEDUP_TTL = 60_000;

    // 外部注入
    this.store = null;

    // CA → 创建者地址（链上创建策略）
    this._creatorMap = new Map();
    this._creatorBuyTriggered = new Set();
    this._firstBuyBuffer = new Map();  // BUY先于CREATE到达时暂存

    // ── 连接状态聚合 ──────────────────────────────────────────────────────────
    this._wasConnected = false;
    const onChannelStatus = () => {
      const anyConnected = this._chCreate.connected || this._chTrade.connected;
      if (anyConnected && !this._wasConnected) {
        this._wasConnected = true;
        this.emit('connected');
      } else if (!anyConnected && this._wasConnected) {
        this._wasConnected = false;
        this.emit('disconnected');
      }
    };

    // ── WSS-1：TOKEN_CREATE 专用，3条RPC自动轮换 ─────────────────────────────
    this._chCreate = new RotatingWssChannel(
      'WSS-1(Create)',
      NETWORK.bscWssCreatePool,
      [
        // 只订阅 TOKEN_CREATE 事件
        { address: FACTORY_ADDRESS, topics: [TOKEN_CREATE_TOPIC] },
      ],
      (log) => this._dispatchCreate(log),
      onChannelStatus
    );

    // ── WSS-2：TOKEN_BUY + TOKEN_SELL（只监控内盘交易，不订阅Transfer）──────
    this._chTrade = new WssChannel(
      'WSS-2(Trade)',
      NETWORK.bscWssBuy,
      [
        // TOKEN_BUY
        { address: FACTORY_ADDRESS, topics: [TOKEN_BUY_TOPIC] },
        // TOKEN_SELL
        { address: FACTORY_ADDRESS, topics: [TOKEN_SELL_TOPIC] },
      ],
      (log) => this._dispatchTrade(log),
      onChannelStatus
    );

    const poolUrls = NETWORK.bscWssCreatePool.join(' | ');
    console.log(`[Chain] 初始化 | WSS-1(Create,3条轮换) + WSS-2(Trade) | 监控钱包: ${this.watchWallets.size}`);
    console.log(`[Chain]   WSS-1 RPC池: ${poolUrls}`);
    console.log(`[Chain]   WSS-2: ${NETWORK.bscWssBuy}`);
  }

  start() {
    this.running = true;
    this._chCreate.start();
    this._chTrade.start();
  }

  stop() {
    this.running = false;
    this._chCreate.stop();
    this._chTrade.stop();
  }

  get connected() {
    return this._chCreate.connected || this._chTrade.connected;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  WSS-1 分发：只处理 TOKEN_CREATE
  // ═══════════════════════════════════════════════════════════════════════════

  _dispatchCreate(log) {
    if (!log || !log.topics || !log.topics.length) return;
    const topic0 = log.topics[0].toLowerCase();
    if (topic0 !== TOKEN_CREATE_TOPIC.toLowerCase()) return;
    const logAddr = (log.address || '').toLowerCase();
    if (logAddr !== FACTORY_LOWER) return;

    this._handleCreate({
      topics: log.topics,
      data: log.data || '0x',
      transactionHash: log.transactionHash || '',
      blockNumber: log.blockNumber ? parseInt(log.blockNumber, 16) : null,
      address: logAddr,
    });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  WSS-2 分发：TOKEN_BUY、TOKEN_SELL、Transfer
  // ═══════════════════════════════════════════════════════════════════════════

  _dispatchTrade(log) {
    if (!log || !log.topics || !log.topics.length) return;
    const logAddr = (log.address || '').toLowerCase();
    const topic0  = log.topics[0].toLowerCase();

    const normalized = {
      topics: log.topics,
      data: log.data || '0x',
      transactionHash: log.transactionHash || '',
      blockNumber: log.blockNumber ? parseInt(log.blockNumber, 16) : null,
      address: logAddr,
    };

    if (logAddr === FACTORY_LOWER) {
      if (topic0 === TOKEN_BUY_TOPIC.toLowerCase()) {
        this._handleTrade(normalized, 'buy');
      } else if (topic0 === TOKEN_SELL_TOPIC.toLowerCase()) {
        this._handleTrade(normalized, 'sell');
      }
      return;
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  TOKEN_CREATE → store.register
  //  WSS-1 可能有多条RPC同时到达，_seenCA 去重保证只处理一次
  // ═══════════════════════════════════════════════════════════════════════════

  _handleCreate(log) {
    const timeStr = formatBeijingTimeMs(new Date());
    let decoded;
    try { decoded = this._abiCoder.decode(CREATE_TYPES, log.data); } catch (e) {
      console.warn('[Chain] CREATE 解码失败:', e.message);
      return;
    }

    const creator = (decoded[0] || '').toString().toLowerCase();
    const ca      = (decoded[1] || '').toString().toLowerCase();
    const name    = (decoded[3] || '').toString();
    const ticker  = (decoded[4] || '').toString();
    if (!ca || ca === ZERO_ADDR) return;

    // _seenCA 去重：3条RPC可能重复推送同一CREATE事件
    if (this._seenCA.has(ca)) return;
    this._seenCA.add(ca);

    // 记录 CA → 创建者（策略二用）
    if (creator && creator !== ZERO_ADDR) {
      this._creatorMap.set(ca, creator);
      // 内存保护
      if (this._creatorMap.size > 5000) {
        const keys = Array.from(this._creatorMap.keys()).slice(0, 1000);
        for (const k of keys) this._creatorMap.delete(k);
      }

      // 回放首笔买入缓冲（BUY先于CREATE到达的情况）
      if (this._firstBuyBuffer.has(ca) && !this._creatorBuyTriggered.has(ca)) {
        const buffered = this._firstBuyBuffer.get(ca);
        this._firstBuyBuffer.delete(ca);
        if (buffered.userAddress === creator) {
          this._creatorBuyTriggered.add(ca);
          console.log(`[Chain] 🏗️♻️ 回放创建者首笔买入 | CA:${ca.slice(0,10)}... | ${buffered.bnbAmount} BNB`);
          if (this.store) this.store.setCreatorBuy(ca, creator, buffered.bnbAmount);
        }
      }
    }

    console.log(`[Chain] 🔗 CREATE | ${ticker} | ${name} | CA:${ca.slice(0,10)}... | creator:${creator.slice(0,10)}... | ${timeStr}`);

    // ① 热路径：立即注册到 store（内部触发策略二匹配）
    if (this.store) {
      this.store.register(ca, ticker, name, timeStr);
    }

    // ② 冷路径：通知前端（低优先级）
    setImmediate(() => this.emit('new_token', { ca, ticker, name, time: timeStr }));
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  TOKEN_BUY / TOKEN_SELL → store.updatePrice + 钱包信号
  // ═══════════════════════════════════════════════════════════════════════════

  _handleTrade(log, side) {
    const timeStr = formatBeijingTimeMs(new Date());
    if (!log.data || log.data.length < 2 + 8 * 64) return;
    let decoded;
    try { decoded = this._abiCoder.decode(TRADE_TYPES, log.data); } catch (e) {
      console.warn('[Chain] TRADE 解码失败:', e.message);
      return;
    }

    const tokenAddress = decoded[0].toLowerCase();
    const userAddress  = decoded[1].toLowerCase();
    const tokenAmount  = ethers.formatEther(decoded[3]);
    const bnbAmount    = ethers.formatEther(decoded[4]);
    if (!tokenAddress || tokenAddress === ZERO_ADDR) return;

    console.log(`[Chain] 💰 ${side === 'buy' ? '🟢买入' : '🔴卖出'} | CA:${tokenAddress.slice(0,10)}... | BNB:${bnbAmount} | ${timeStr}`);

    // ━━━ 优先级①：创建者首笔买入检测（策略二辅助条件）━━━
    if (side === 'buy' && this.store && !this._creatorBuyTriggered.has(tokenAddress)) {
      const creator = this._creatorMap.get(tokenAddress);
      if (creator && creator === userAddress) {
        this._creatorBuyTriggered.add(tokenAddress);
        const bnbNum = parseFloat(bnbAmount) || 0;
        console.log(`[Chain] 🏗️ 创建者首笔买入 | CA:${tokenAddress.slice(0,10)}... | ${bnbNum} BNB`);
        this.store.setCreatorBuy(tokenAddress, userAddress, bnbNum);
      } else if (!creator) {
        // CREATE 未到达，缓冲首笔
        if (!this._firstBuyBuffer.has(tokenAddress)) {
          this._firstBuyBuffer.set(tokenAddress, { userAddress, bnbAmount: parseFloat(bnbAmount) || 0 });
        }
      }
    }

    // ━━━ 优先级②：更新市值（触发策略一条件④）━━━
    if (this.store) {
      this.store.updatePrice(tokenAddress, bnbAmount, tokenAmount);
    }

    // ━━━ 优先级③：监控钱包信号（触发策略一条件③）━━━
    if (this.watchWallets.has(userAddress) && this.store) {
      const walletName = this.watchWallets.get(userAddress);
      this.store.addWalletSignal(tokenAddress, {
        time: timeStr,
        walletName,
        walletAddress: userAddress,
        action: side === 'buy' ? '买入' : '卖出',
        bnbAmount,
        tokenAmount,
        txHash: log.transactionHash,
        marketCapUSD: 0,
      });

      // 冷路径：前端钱包监控tab推送
      setImmediate(() => {
        const token = this.store.get(tokenAddress);
        this.emit('wallet_trade', {
          time: timeStr, walletName, walletAddress: userAddress,
          action: side === 'buy' ? '买入' : '卖出',
          tokenAddress, tokenSymbol: token?.symbol || token?.fmSymbol || '',
          tokenName: token?.name || '', tokenImage: token?.image || '',
          bnbAmount, tokenAmount, txHash: log.transactionHash,
          blockNumber: log.blockNumber, source: 'factory',
          marketCapUSD: token?.marketCapUSD || 0,
        });
      });
    }

    // ━━━ 优先级④：交易明细写入 store（前端K线面板）━━━
    if (this.store) {
      this.store.addTrade(tokenAddress, {
        source: 'chain', side, sideLabel: side === 'buy' ? '买入' : '卖出',
        tokenAddress, userAddress, bnbAmount, tokenAmount,
        txHash: log.transactionHash, blockNumber: log.blockNumber, time: timeStr,
        tokenSymbol: '', tokenName: '', tokenImage: '',
        tokenTwitterUrl: '', tokenTwitterDisp: '', tokenTwitterHref: '',
        marketCapUSD: 0,
      });
    }
  }

}

module.exports = ChainWatcher;
