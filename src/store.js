'use strict';
/**
 * store.js — TokenStore 唯一数据中心 + 四条件匹配引擎
 *
 * 核心职责：
 *   1. tokenMap (Map<CA, Token>) 是全系统唯一的数据真相源
 *   2. 所有数据写入通过 register / enrich / updatePrice / addWalletSignal 方法
 *   3. 每次写入自动调用 _tryMatch(token)，四条件合取满足即触发买入
 *   4. 对外 emit 事件供前端推送和持久化消费（低优先级）
 *
 * 四条件匹配（合取，全满足才触发）：
 *   ① mediaMatched:     twitterUrl 包含 targetAccounts 中任一字符串
 *   ② mediaTimeMatched: |twitterCreatedAt - programGetTime| < 3 分钟
 *   ③ walletBuy:        监控钱包对该 CA 有买入记录
 *   ④ marketCap:        0 < MC < 10,000 USD
 *
 * 设计原则：
 *   - 匹配第一：_tryMatch 是纯同步函数，0ms 延迟
 *   - 数据一次获取多处复用：写入即存储，后续只读布尔值
 *   - CA 为唯一索引：所有数据源以 CA 关联合并
 */

const { EventEmitter } = require('events');
const { formatBeijingTimeMs, parseBeijingTime, extractTweetId, TWITTER_EPOCH } = require('./utils');

// tokenMap 内存上限
const TOKEN_MAP_MAX = 3000;
// 裁剪周期（6小时）
const TOKEN_MAP_TRIM_INTERVAL = 6 * 3600_000;

class TokenStore extends EventEmitter {
  /**
   * @param {object} opts
   * @param {string[]} opts.targetAccounts - 媒体匹配目标字符串列表
   * @param {number}   opts.timeWindowMinutes - 媒体时间窗口（默认3分钟）
   * @param {number}   opts.bnbPriceUSD - BNB 美元价格
   * @param {function} opts.onMatched - 匹配成功回调（同步调用，最高优先级）
   */
  constructor(opts = {}) {
    super();
    this.tokenMap = new Map();  // ca(lowercase) → token

    // 配置
    this.targetAccounts = (opts.targetAccounts || []).map(s => String(s).trim().toLowerCase()).filter(Boolean);
    this.timeWindowMinutes = opts.timeWindowMinutes || 3;
    this.bnbPriceUSD = opts.bnbPriceUSD || 580;

    // ── 链上创建事件策略配置 ──────────────────────────────────────────────
    this.createKeywords = (opts.createKeywords || []).map(s => String(s).trim().toLowerCase()).filter(Boolean);
    this.createBuyMinBNB = opts.createBuyMinBNB || 0.03;
    this.createBuyMaxBNB = opts.createBuyMaxBNB || 2;

    // 匹配成功的同步回调（直接调用 trader.onMatched，不经过事件循环）
    this._onMatched = opts.onMatched || null;
    // 链上创建事件策略匹配回调（独立于四条件策略）
    this._onCreateMatched = opts.onCreateMatched || null;

    // ━━━ 名称去重：已购买代币的 ticker/name/fmSymbol/fmName 集合 ━━━
    // 相同名称不再重复购买
    this._boughtNames = new Set();

    // 默认 totalSupply（four.meme 标准值）
    this._defaultSupply = 1073972602.739726;

    // ━━━ 早到缓冲：Buy/Sell 事件可能先于 CREATE 到达 ━━━
    // ca → { trades: [], prices: [], signals: [] }
    this._earlyTradeBuffer = new Map();
    this._EARLY_BUFFER_TTL = 60_000;  // 60秒过期
    this._EARLY_BUFFER_MAX = 500;     // 每个CA最多缓存条数

    // 定期裁剪（防长期运行内存增长）
    this._trimTimer = setInterval(() => this._trim(), TOKEN_MAP_TRIM_INTERVAL);
    if (this._trimTimer.unref) this._trimTimer.unref();

    // 定期清理早到缓冲中过期数据
    this._earlyBufferCleanTimer = setInterval(() => this._cleanEarlyBuffer(), 30_000);
    if (this._earlyBufferCleanTimer.unref) this._earlyBufferCleanTimer.unref();
  }

  /** 更新 BNB 价格（由外部定时更新） */
  setBNBPrice(price) {
    if (price > 0) this.bnbPriceUSD = price;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  数据写入方法（每个末尾都调用 _tryMatch）
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * 注册新代币（来自链上 TOKEN_CREATE）
   * @returns {object|null} 新创建的 token 对象，重复则返回 null
   */
  register(ca, ticker, name, programGetTime) {
    if (!ca || this.tokenMap.has(ca)) return null;

    const token = {
      tokenId: ca,
      tokenAddress: ca,
      symbol: ticker || '',
      name: name || '',
      programGetTime,
      arrivalTime: new Date(),
      // 媒体字段（待 enrich 补充）
      image: null,
      twitterUrl: null,
      twitterDisplay: null,
      twitterHref: null,
      twitterUsername: null,
      mediaAddressTime: null,
      twitterCreatedAt: null,
      twitterContent: null,
      twitterContentTime: null,
      fmSymbol: null,
      fmName: null,
      tamount: null,
      // 匹配状态
      mediaMatched: false,
      mediaTimeMatched: false,
      _mediaMatchedAccount: null,
      _mediaTimeDiffMin: null,
      walletSignals: [],
      marketCapUSD: 0,
      _lastPrice: 0,
      matchReason: null,
      // 媒体竞速结果（谁先到达记录来源+耗时）
      mediaSource: null,       // 'wss' | 'api'
      mediaLatencyMs: null,    // 从 arrivalTime 到媒体数据首次到达的耗时(ms)
      // 交易状态
      bought: false,
      sold1: false, sold2: false, sold3: false,
      txBuy: null, txSell1: null, txSell2: null, txSell3: null,
      buyStatus: null,
      _enriched: false,
      // ── 链上创建事件策略字段 ──────────────────────────────────────────────
      _createKeywordMatched: false,    // ticker/name 包含关键词
      _createKeyword: null,            // 命中的关键词
      _creatorAddress: null,           // 创建者地址
      _creatorBuyBNB: 0,              // 创建者首笔买入 BNB
      _createStrategyMatched: false,   // 链上创建策略是否已匹配
      _createMatchReason: null,        // 匹配原因
    };

    this.tokenMap.set(ca, token);
    this.emit('registered', token);

    // ━━━ 链上创建事件策略：检查关键词匹配 ━━━
    this._evalCreateKeyword(token);

    // ━━━ 回放早到缓冲（Buy/Sell 先于 CREATE 到达的数据）━━━
    this._replayEarlyBuffer(ca);

    // 新注册时条件不可能满足，但保持一致性仍调用
    this._tryMatch(token);
    return token;
  }

  /**
   * 补充媒体数据（来自 FourMeme TOKEN_EVENT）
   * 内部计算条件①②并存储
   */
  enrich(ca, data) {
    const token = this.tokenMap.get(ca);
    if (!token) return null;

    let changed = false;

    // 图片
    if (data.image && !token.image) {
      token.image = data.image;
      changed = true;
    }

    // symbol / name（FourMeme 提供的，区别于链上 ticker）
    if (data.fmSymbol) {
      const s = String(data.fmSymbol).trim();
      if (s && token.fmSymbol !== s) { token.fmSymbol = s; changed = true; }
    }
    if (data.fmName) {
      const n = String(data.fmName).trim();
      if (n && !token.fmName) { token.fmName = n; changed = true; }
    }

    // totalSupply
    if (data.tamount && !token.tamount) {
      token.tamount = data.tamount;
      changed = true;
    }

    // ━━━ 记录媒体竞速结果（即使没有 twitterUrl，只要有有效数据到达就记录）━━━
    if (!token.mediaSource && data._source && changed) {
      token.mediaSource = data._source;
      token.mediaLatencyMs = token.arrivalTime instanceof Date
        ? Date.now() - token.arrivalTime.getTime()
        : 0;
    }

    // 媒体链接（核心：条件①②的数据来源）
    if (data.twitterUrl && !token.twitterUrl) {
      token.twitterUrl = data.twitterUrl;
      token.twitterDisplay = data.twitterDisplay || null;
      token.twitterHref = data.twitterHref || null;
      token.twitterUsername = data.twitterUsername || null;
      token.mediaAddressTime = data.mediaAddressTime || formatBeijingTimeMs(new Date());
      changed = true;

      // ━━━ 记录媒体竞速结果（第一个到达的来源 + 耗时）━━━
      if (!token.mediaSource) {
        token.mediaSource = data._source || 'wss';
        token.mediaLatencyMs = token.arrivalTime instanceof Date
          ? Date.now() - token.arrivalTime.getTime()
          : 0;
      }

      // 从 tweet ID 推算媒体创建时间
      const tweetId = extractTweetId(token.twitterUrl);
      if (tweetId) {
        try {
          const ts = Number((BigInt(tweetId) >> 22n) + TWITTER_EPOCH);
          token.twitterCreatedAt = formatBeijingTimeMs(new Date(ts));
        } catch (_) {}
      }

      // ━━━ 计算条件① 媒体字符串匹配 ━━━
      this._evalMediaMatch(token);
      // ━━━ 计算条件② 时间差 < 3分钟 ━━━
      this._evalTimeMatch(token);
    }

    // 媒体链接重写（匿名URL解析后回调）
    if (data.twitterUrlRewrite && token.twitterUrl !== data.twitterUrlRewrite) {
      token.twitterUrl = data.twitterUrlRewrite;
      if (data.twitterHref) token.twitterHref = data.twitterHref;
      if (data.twitterDisplay) token.twitterDisplay = data.twitterDisplay;
      if (data.twitterUsername) token.twitterUsername = data.twitterUsername;
      changed = true;
      // 重新评估条件①（URL 变了，匹配结果可能变）
      this._evalMediaMatch(token);
    }

    // 推文内容（异步到达，不影响匹配，仅前端展示）
    if (data.twitterContent && !token.twitterContent) {
      token.twitterContent = data.twitterContent;
      token.twitterContentTime = data.twitterContentTime || formatBeijingTimeMs(new Date());
      changed = true;
    }

    if (changed) {
      this.emit('enriched', token);
      this._tryMatch(token);
    }
    return token;
  }

  /**
   * 更新价格/市值（来自链上 TOKEN_BUY/SELL）
   * 内部计算条件④
   * 如果 CA 未注册，缓存到 earlyTradeBuffer，等待 register 后回放
   */
  updatePrice(ca, bnbAmount, tokenAmount) {
    const token = this.tokenMap.get(ca);
    if (!token) {
      // ━━━ 早到缓冲：CA 尚未注册，先缓存 ━━━
      this._bufferEarly(ca, 'price', { bnbAmount, tokenAmount });
      return null;
    }

    const bnbNum = parseFloat(bnbAmount) || 0;
    const tokNum = parseFloat(tokenAmount) || 0;
    if (bnbNum <= 0 || tokNum <= 0) return token;

    const priceBNB = bnbNum / tokNum;
    token._lastPrice = priceBNB;

    const totalSupply = parseFloat(token.tamount) || this._defaultSupply;
    const symbol = (token.symbol || '').toUpperCase();
    // 稳定币定价不乘 BNB 价格
    const priceUSD = ['USDT', 'USDC', 'USD1', 'U'].includes(symbol)
      ? priceBNB * totalSupply
      : priceBNB * totalSupply * this.bnbPriceUSD;

    token.marketCapUSD = priceUSD;

    // 条件④在 _tryMatch 内直接判断 marketCapUSD，无需额外标记
    this._tryMatch(token);

    // 价格事件（供止盈 + 前端）— 低优先级
    this.emit('price_updated', { token, priceBNB, marketCapUSD: priceUSD });
    return token;
  }

  /**
   * 添加监控钱包买入信号（来自链上 TOKEN_BUY 或 Transfer）
   * 内部满足条件③
   * 如果 CA 未注册，缓存到 earlyTradeBuffer，等待 register 后回放
   */
  addWalletSignal(ca, signal) {
    const token = this.tokenMap.get(ca);
    if (!token) {
      // ━━━ 早到缓冲：CA 尚未注册，先缓存 ━━━
      if (signal && signal.action === '买入') {
        this._bufferEarly(ca, 'signal', signal);
      }
      return null;
    }
    if (!signal || signal.action !== '买入') return token;

    // 去重：同一 txHash 不重复
    if (signal.txHash && token.walletSignals.some(s => s.txHash === signal.txHash)) return token;

    token.walletSignals.push({
      time: signal.time,
      walletName: signal.walletName,
      walletAddress: signal.walletAddress,
      bnbAmount: signal.bnbAmount,
      tokenAmount: signal.tokenAmount,
      txHash: signal.txHash,
      marketCapUSD: signal.marketCapUSD || token.marketCapUSD || 0,
    });

    // 条件③满足 → _tryMatch
    this._tryMatch(token);
    this.emit('wallet_signal', { token, signal });
    return token;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  条件评估（纯同步，写入 token 布尔标记）
  // ═══════════════════════════════════════════════════════════════════════════

  /** 条件①：媒体URL是否包含目标账号字符串 */
  _evalMediaMatch(token) {
    const url = (token.twitterUrl || '').toLowerCase();
    if (!url) { token.mediaMatched = false; return; }
    const matched = this.targetAccounts.find(acc => url.includes(acc));
    token.mediaMatched = !!matched;
    token._mediaMatchedAccount = matched || null;
  }

  /** 条件②：媒体创建时间与代币创建时间差 < N 分钟 */
  _evalTimeMatch(token) {
    const tweetCreated = token.twitterCreatedAt;
    const progTime = token.programGetTime;
    if (!tweetCreated || !progTime) { token.mediaTimeMatched = false; return; }

    const tTweet = parseBeijingTime(tweetCreated);
    const tProg = parseBeijingTime(progTime);
    if (!tTweet || !tProg) { token.mediaTimeMatched = false; return; }

    const diffMin = Math.abs(tProg - tTweet) / 60000;
    token.mediaTimeMatched = diffMin < this.timeWindowMinutes;
    token._mediaTimeDiffMin = diffMin;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  名称去重：已购买代币的 ticker/name/fmSymbol/fmName 不重复购买
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * 检查该 token 的任一名称是否已在 _boughtNames 中
   * @returns {boolean} true = 重复，应跳过
   */
  _isNameDuplicate(token) {
    const names = this._getTokenNames(token);
    for (const n of names) {
      if (this._boughtNames.has(n)) return true;
    }
    return false;
  }

  /**
   * 记录已购买代币的所有名称到 _boughtNames
   */
  _recordBoughtNames(token) {
    const names = this._getTokenNames(token);
    for (const n of names) {
      this._boughtNames.add(n);
    }
  }

  /**
   * 提取 token 去重标识（ticker + name，原样比较不做小写处理）
   */
  _getTokenNames(token) {
    const raw = [
      token.symbol,     // ticker（链上 CREATE 事件的 ticker）
      token.name,       // name（链上 CREATE 事件的 name）
    ];
    const names = [];
    for (const r of raw) {
      if (!r) continue;
      const n = String(r).trim();
      if (n && n !== '—' && n !== '加载中…') names.push(n);
    }
    return names;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  链上创建事件策略 — 关键词 + 创建者买入金额
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * 评估 ticker/name 是否包含关键词列表中的任一词
   */
  _evalCreateKeyword(token) {
    if (!this.createKeywords || !this.createKeywords.length) return;

    const ticker = (token.symbol || '').toLowerCase();
    const name = (token.name || '').toLowerCase();
    const fmName = (token.fmName || '').toLowerCase();
    const fmSymbol = (token.fmSymbol || '').toLowerCase();

    // 合并所有名称用于匹配
    const combined = `${ticker}|${name}|${fmName}|${fmSymbol}`;

    for (const kw of this.createKeywords) {
      if (combined.includes(kw)) {
        token._createKeywordMatched = true;
        token._createKeyword = kw;
        return;
      }
    }
    token._createKeywordMatched = false;
  }

  /**
   * 设置创建者首笔买入信息（由 chain.js 在检测到创建者买入时调用）
   * 条件：创建者买入 > 0.03 BNB 且 < 2 BNB
   * 满足则立即触发 _tryCreateMatch
   *
   * @param {string} ca - 代币合约地址
   * @param {string} creatorAddress - 创建者地址
   * @param {number|string} bnbAmount - 创建者买入 BNB 数量
   */
  setCreatorBuy(ca, creatorAddress, bnbAmount) {
    const token = this.tokenMap.get(ca);
    if (!token) {
      // 早到缓冲
      this._bufferEarly(ca, 'creatorBuy', { creatorAddress, bnbAmount });
      return null;
    }

    const bnbNum = parseFloat(bnbAmount) || 0;
    token._creatorAddress = creatorAddress;
    token._creatorBuyBNB = bnbNum;

    console.log(`[Store] 🏗️ 创建者买入 | CA:${ca.slice(0,10)}... | creator:${creatorAddress.slice(0,10)}... | ${bnbNum} BNB`);

    // 尝试链上创建策略匹配
    this._tryCreateMatch(token);
    return token;
  }

  /**
   * 链上创建事件策略匹配（两条件合取）：
   *   ① ticker/name/shortname 包含关键词
   *   ② 创建者首笔买入 > 0.03 BNB 且 < 2 BNB
   * 全满足 → 毫秒级买入
   */
  _tryCreateMatch(token) {
    if (token._createStrategyMatched) return;  // 已匹配过
    if (token.bought) return;                  // 已被其他策略买入

    // 条件①：关键词
    if (!token._createKeywordMatched) return;

    // 条件②：创建者买入金额在范围内
    const buyBNB = token._creatorBuyBNB;
    if (!(buyBNB > this.createBuyMinBNB && buyBNB < this.createBuyMaxBNB)) return;

    // ━━━ 名称去重：相同 ticker/name/fmSymbol/fmName 不重复购买 ━━━
    if (this._isNameDuplicate(token)) {
      console.log(`[Store] 🚫 名称重复跳过: ${token.symbol || token.name} | CA:${token.tokenAddress}`);
      return;
    }

    // 🎯 两条件全满足
    token._createStrategyMatched = true;
    token._createMatchReason =
      `关键词[${token._createKeyword}] | ` +
      `创建者买入 ${buyBNB.toFixed(4)} BNB (${this.createBuyMinBNB}~${this.createBuyMaxBNB})`;

    console.log(`[Store] 🎯🏗️ 链上创建策略匹配: ${token.symbol} | CA:${token.tokenAddress} | ${token._createMatchReason}`);

    // 记录已购买名称（防后续重复）
    this._recordBoughtNames(token);

    // 优先级③：同步直调买入（不经过事件循环）
    if (this._onCreateMatched) {
      this._onCreateMatched(token);
    }

    // 低优先级：异步通知前端+持久化
    this.emit('create_matched', token);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  四条件匹配（纯同步，~0.01ms）
  // ═══════════════════════════════════════════════════════════════════════════

  _tryMatch(token) {
    if (token.matchReason) return;                        // 已匹配过
    if (!token.mediaMatched) return;                      // ① 媒体未命中
    if (!token.mediaTimeMatched) return;                  // ② 时差超限
    if (!token.walletSignals || !token.walletSignals.length) return; // ③ 无钱包买入
    const mc = token.marketCapUSD;
    if (!(mc > 0 && mc < 10000)) return;                  // ④ 市值不在范围

    // ━━━ 名称去重：相同 ticker/name/fmSymbol/fmName 不重复购买 ━━━
    if (this._isNameDuplicate(token)) {
      console.log(`[Store] 🚫 名称重复跳过: ${token.symbol || token.name} | CA:${token.tokenAddress}`);
      return;
    }

    // 🎯 四条件全满足
    token.matchReason =
      `媒体命中[${token._mediaMatchedAccount || '—'}] | ` +
      `时差 ${Number(token._mediaTimeDiffMin || 0).toFixed(1)}min | ` +
      `钱包买入 ${token.walletSignals.length} 笔 | ` +
      `市值 $${Math.round(mc).toLocaleString()}`;

    console.log(`[Store] \u{1F3AF} 匹配成功: ${token.symbol} | CA:${token.tokenAddress} | ${token.matchReason}`);

    // 记录已购买名称（防后续重复）
    this._recordBoughtNames(token);

    // 优先级③：同步直调买入（不经过事件循环）
    if (this._onMatched) {
      this._onMatched(token);
    }

    // 低优先级：异步通知前端+持久化
    this.emit('matched', token);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  交易明细唯一入口（来自链上 TOKEN_BUY/SELL）
  //  确保每笔交易都被记录，不因时序问题丢失
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * 添加交易明细（链上 buy/sell 事件的完整记录）
   * 如果 CA 未注册，缓存到 earlyTradeBuffer，register 后自动回放并 emit
   *
   * @param {string} ca - 代币合约地址（lowercase）
   * @param {object} trade - 交易完整数据
   * @returns {object|null} token 对象
   */
  addTrade(ca, trade) {
    const token = this.tokenMap.get(ca);
    if (!token) {
      // ━━━ 早到缓冲：CA 尚未注册，先缓存 ━━━
      this._bufferEarly(ca, 'trade', trade);
      return null;
    }

    // 初始化交易明细数组（按需懒创建）
    if (!token.trades) token.trades = [];

    // 去重：同一 txHash + side 不重复
    const dedupKey = `${trade.txHash}|${trade.side}`;
    if (token.trades.some(t => `${t.txHash}|${t.side}` === dedupKey)) return token;

    // 补充 token 信息到 trade 记录（统一从 store 获取最新数据）
    trade.tokenSymbol = token.symbol || token.fmSymbol || trade.tokenSymbol || '';
    trade.tokenName = token.name || token.fmName || trade.tokenName || '';
    trade.tokenImage = token.image || trade.tokenImage || '';
    trade.tokenTwitterUrl = token.twitterUrl || '';
    trade.tokenTwitterDisp = token.twitterDisplay || '';
    trade.tokenTwitterHref = token.twitterHref || '';
    trade.marketCapUSD = token.marketCapUSD || 0;

    token.trades.push(trade);
    // 限制每个代币的交易明细数量
    if (token.trades.length > 200) token.trades.shift();

    // emit 事件供前端推送（低优先级）
    this.emit('trade_added', { token, trade });
    return token;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  早到缓冲管理
  //  Buy/Sell 事件可能先于 CREATE 到达（3条WSS独立时序）
  //  缓冲后在 register 时自动回放，确保零数据丢失
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * 缓存早到事件
   * @param {string} ca
   * @param {'price'|'signal'|'trade'} type
   * @param {object} data
   */
  _bufferEarly(ca, type, data) {
    if (!this._earlyTradeBuffer.has(ca)) {
      this._earlyTradeBuffer.set(ca, { trades: [], prices: [], signals: [], creatorBuys: [], createdAt: Date.now() });
    }
    const bucket = this._earlyTradeBuffer.get(ca);

    if (type === 'price') {
      if (bucket.prices.length < this._EARLY_BUFFER_MAX) bucket.prices.push(data);
    } else if (type === 'signal') {
      if (bucket.signals.length < this._EARLY_BUFFER_MAX) bucket.signals.push(data);
    } else if (type === 'trade') {
      if (bucket.trades.length < this._EARLY_BUFFER_MAX) bucket.trades.push(data);
    } else if (type === 'creatorBuy') {
      if (bucket.creatorBuys.length < 5) bucket.creatorBuys.push(data);
    }
  }

  /**
   * register 后回放早到缓冲：按原始顺序重新执行
   * 保证 updatePrice / addWalletSignal / addTrade 都能命中已注册的 token
   */
  _replayEarlyBuffer(ca) {
    const bucket = this._earlyTradeBuffer.get(ca);
    if (!bucket) return;
    this._earlyTradeBuffer.delete(ca);

    const priceCount = bucket.prices.length;
    const signalCount = bucket.signals.length;
    const tradeCount = bucket.trades.length;
    const creatorBuyCount = (bucket.creatorBuys || []).length;

    if (priceCount + signalCount + tradeCount + creatorBuyCount === 0) return;

    console.log(`[Store] ♻️ 回放早到缓冲 CA:${ca.slice(0, 10)}... | price:${priceCount} signal:${signalCount} trade:${tradeCount} creatorBuy:${creatorBuyCount}`);

    // 先回放创建者买入（触发链上创建策略匹配，毫秒级）
    for (const cb of (bucket.creatorBuys || [])) {
      this.setCreatorBuy(ca, cb.creatorAddress, cb.bnbAmount);
    }
    // 再回放价格（确保市值条件④能计算）
    for (const p of bucket.prices) {
      this.updatePrice(ca, p.bnbAmount, p.tokenAmount);
    }
    // 再回放钱包信号（确保条件③）
    for (const s of bucket.signals) {
      this.addWalletSignal(ca, s);
    }
    // 最后回放交易明细（确保前端能收到完整数据）
    for (const t of bucket.trades) {
      this.addTrade(ca, t);
    }
  }

  /**
   * 定期清理过期的早到缓冲（60秒内没有 CREATE 到达的视为无效）
   */
  _cleanEarlyBuffer() {
    const now = Date.now();
    for (const [ca, bucket] of this._earlyTradeBuffer) {
      if (now - bucket.createdAt > this._EARLY_BUFFER_TTL) {
        this._earlyTradeBuffer.delete(ca);
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  查询方法
  // ═══════════════════════════════════════════════════════════════════════════

  get(ca) { return this.tokenMap.get(ca) || null; }
  has(ca) { return this.tokenMap.has(ca); }
  get size() { return this.tokenMap.size; }

  getAllTokens() {
    return Array.from(this.tokenMap.values()).sort((a, b) => b.arrivalTime - a.arrivalTime);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  内存管理
  // ═══════════════════════════════════════════════════════════════════════════

  _trim() {
    if (this.tokenMap.size <= TOKEN_MAP_MAX) return;

    const sorted = Array.from(this.tokenMap.entries()).sort((a, b) => {
      const ta = a[1].arrivalTime instanceof Date ? a[1].arrivalTime.getTime() : (a[1].arrivalTime || 0);
      const tb = b[1].arrivalTime instanceof Date ? b[1].arrivalTime.getTime() : (b[1].arrivalTime || 0);
      return ta - tb;
    });

    let deleted = 0;
    for (const [ca] of sorted) {
      if (this.tokenMap.size <= TOKEN_MAP_MAX) break;
      const t = this.tokenMap.get(ca);
      if (t && t.bought && !t.sold3) continue; // 持仓不删
      this.tokenMap.delete(ca);
      deleted++;
    }
    if (deleted > 0) {
      console.log(`[Store] \u{1F9F9} 裁剪 ${deleted} 条 | 剩余 ${this.tokenMap.size}`);
    }
  }

  destroy() {
    if (this._trimTimer) { clearInterval(this._trimTimer); this._trimTimer = null; }
    if (this._earlyBufferCleanTimer) { clearInterval(this._earlyBufferCleanTimer); this._earlyBufferCleanTimer = null; }
  }
}

module.exports = TokenStore;
