'use strict';
/**
 * fourmeme.js — FourMeme WSS 独立媒体数据补充
 *
 * 职责（单一且明确）：
 *   通过 wss://ws.four.meme/ws 订阅 @TOKEN_EVENT@0
 *   补充最新创建 MEME 的图片、媒体链接、symbol、name、totalSupply
 *   数据到达后调用 store.enrich(ca, data) 写入唯一数据中心
 *
 * 与 Chain WSS 完全独立：
 *   - 各自连接、各自重连、互不影响
 *   - FourMeme 断线不影响链上匹配/买卖
 *   - Chain 断线不影响媒体数据缓冲
 *
 * 匿名 URL 处理：
 *   当 twitterUrl 为 x.com/i/status/xxx 格式时，并发：
 *   ① oEmbed (~500ms，免费) 与 ② socialdata (~4s，付费) 竞速
 *   谁先解析出账号名，就通过 store.enrich 重写 URL 触发条件①重评估
 *
 * 早到事件缓冲：
 *   TOKEN_EVENT 可能早于链上 TOKEN_CREATE 到达
 *   先缓存到 earlyBuffer，store.register 后回放
 */

const WebSocket = require('ws');
const https = require('https');
const { EventEmitter } = require('events');
const { formatBeijingTimeMs, extractTweetId, toReadableTwitter } = require('./utils');
const { NETWORK, API } = require('./config');

// 常量
const FOUR_MEME_WSS = NETWORK.fourMemeWss;
const EARLY_BUFFER_MAX = 2000;
const EARLY_BUFFER_TTL = 5 * 60_000; // 5分钟

class FourMemeWatcher extends EventEmitter {
  constructor() {
    super();
    this.running = false;
    this.reconnecting = false;
    this.ws = null;
    this._reconnectTimer = null;
    this._pingTimer = null;
    this._pongTimeout = null;

    // 早到事件缓冲：ca → [{data, arrivedAt}]
    this._earlyBuffer = new Map();

    // 推文内容缓存 + 并发去重
    this._tweetCache = new Map();
    this._tweetFetching = new Map();

    this._apiKey = API.socialDataKey;

    // API 竞速去重：ca → true（已从任一通道拿到数据则不再处理）
    this._apiResolved = new Set();

    // 外部注入
    this.store = null; // TokenStore 引用
  }

  start() { this.running = true; this._connect(); }
  stop() {
    this.running = false;
    this._stopPing();
    this._clearReconnect();
    if (this.ws) { try { this.ws.terminate(); } catch (_) {} this.ws = null; }
  }

  /**
   * 注册 CA（链上 new_token 后调用）
   * 仅回放 WSS 早到缓冲事件（API 轮询已移至 index.js 独立管理）
   */
  registerCA(ca) {
    // 回放 WSS 早到缓冲
    const bucket = this._earlyBuffer.get(ca);
    if (bucket && bucket.length) {
      console.log(`[FourMeme] ♻️ 回放 ${bucket.length} 条早到事件 | CA:${ca.slice(0, 10)}...`);
      this._earlyBuffer.delete(ca);
      for (const entry of bucket) {
        this._processTokenEvent(entry.data, true);
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  WebSocket 连接管理
  // ═══════════════════════════════════════════════════════════════════════════

  _connect() {
    if (!this.running) return;
    this._clearReconnect();
    this.reconnecting = false;

    console.log('[FourMeme] 连接:', FOUR_MEME_WSS);
    let ws;
    try {
      ws = new WebSocket(FOUR_MEME_WSS, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Origin': 'https://four.meme',
        },
      });
    } catch (err) {
      console.error('[FourMeme] WS 创建失败:', err.message);
      this._scheduleReconnect();
      return;
    }
    this.ws = ws;

    ws.on('open', () => {
      if (!this.running) { ws.terminate(); return; }
      console.log('[FourMeme] ✅ 已连接');
      this.emit('connected');
      ws.send(JSON.stringify({ method: 'SUBSCRIBE', params: ['@TOKEN_EVENT@0'], id: 1 }));
      ws.send(JSON.stringify({ method: 'SUBSCRIBE', params: ['@TICKER_EVENT'], id: 2 }));
      this._startPing(ws);
    });

    ws.on('pong', () => {
      if (this._pongTimeout) { clearTimeout(this._pongTimeout); this._pongTimeout = null; }
    });

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch (_) { return; }
      if (msg.event === '@TOKEN_EVENT@0' && msg.data) {
        this._processTokenEvent(msg.data, false);
      } else if (msg.event === '@TICKER_EVENT' && msg.data) {
        this._processTickerEvent(msg.data);
      }
    });

    ws.on('close', (code) => {
      console.warn(`[FourMeme] 断开 (code=${code})，1s 后重连...`);
      this._stopPing();
      this.ws = null;
      this.emit('disconnected');
      this._scheduleReconnect();
    });

    ws.on('error', (err) => {
      console.error('[FourMeme] 错误:', err.message);
    });
  }

  _scheduleReconnect() {
    if (!this.running || this.reconnecting) return;
    this.reconnecting = true;
    this._clearReconnect();
    this._reconnectTimer = setTimeout(() => { if (this.running) this._connect(); }, 1000);
  }

  _clearReconnect() {
    if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
  }

  _startPing(ws) {
    this._stopPing();
    this._pingTimer = setInterval(() => {
      if (!ws || ws.readyState !== WebSocket.OPEN) return;
      ws.ping();
      this._pongTimeout = setTimeout(() => {
        console.warn('[FourMeme] pong 超时，断开重连...');
        try { ws.terminate(); } catch (_) {}
      }, 5000);
    }, 20_000);
  }

  _stopPing() {
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
    if (this._pongTimeout) { clearTimeout(this._pongTimeout); this._pongTimeout = null; }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  TICKER_EVENT 处理 — BNB 实时价格
  // ═══════════════════════════════════════════════════════════════════════════

  _processTickerEvent(data) {
    if (!data || typeof data !== 'object') return;
    if (data.symbol !== 'BNB_MPCUSDT') return;
    const price = parseFloat(data.price);
    if (price > 0) {
      this.emit('bnb_price', price);
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  TOKEN_EVENT 处理
  //  核心：提取 image + twitterUrl + symbol → store.enrich(ca, data)
  // ═══════════════════════════════════════════════════════════════════════════

  _processTokenEvent(data, fromBuffer) {
    if (!data || typeof data !== 'object') return;

    const ca = (data.tokenAddress || data.address || '').toLowerCase();
    if (!ca) return;

    const receiveTime = formatBeijingTimeMs(new Date());

    // store 中没有该 CA → 链上 CREATE 还没到，缓冲等待
    if (!this.store || !this.store.has(ca)) {
      if (!fromBuffer) {
        this._bufferEvent(ca, data);
        console.log(`[FourMeme] ⏳ 早到缓冲 CA:${ca.slice(0, 10)}...`);
      }
      return;
    }

    const token = this.store.get(ca);
    if (token._enriched) {
      // 已处理过（可能被 API 竞速先处理），只补充可能缺失的 symbol/name
      if (data.symbol || data.name) {
        this.store.enrich(ca, {
          fmSymbol: data.symbol || null,
          fmName: data.name || data.tokenName || null,
        });
      }
      return;
    }

    // 如果 API 已经先拿到 twitterUrl，标记 _enriched 但仍补充其他字段
    const apiAlreadyResolved = this._apiResolved.has(ca);
    token._enriched = true;

    // 提取字段
    const rawTwitter = data.twitterUrl || data.webUrl || null;
    const rawImage = data.image || data.img || null;
    const rawSymbol = data.symbol || null;
    const rawName = data.name || data.tokenName || null;
    const rawTamount = data.tamount || data.totalSupply || null;

    // 构建 enrich 数据包
    const enrichData = {
      image: rawImage || null,
      fmSymbol: rawSymbol || null,
      fmName: rawName || null,
      tamount: rawTamount || null,
      mediaAddressTime: receiveTime,
      _source: 'wss',
    };

    // 处理媒体链接（如果 API 已拿到 twitterUrl，不覆盖）
    if (rawTwitter && !apiAlreadyResolved) {
      const readable = toReadableTwitter(rawTwitter);
      enrichData.twitterUrl = rawTwitter;
      enrichData.twitterDisplay = readable.display;
      enrichData.twitterHref = readable.href;
      enrichData.twitterUsername = readable.username;
    }

    // 写入 store（内部会计算条件①② + 触发 _tryMatch）
    this.store.enrich(ca, enrichData);

    // 计算 WSS 通道到达时间差（相对于 programGetTime）
    const wssLatency = Date.now() - (token.arrivalTime instanceof Date ? token.arrivalTime.getTime() : Date.now());
    // 通知前端：WSS 通道到达
    this.emit('wss_enriched', { ca, latency: wssLatency, source: 'wss', twitterUrl: rawTwitter || null });

    console.log(`[FourMeme] 📦${fromBuffer ? '(回放)' : ''} CA:${ca.slice(0, 10)}... | tw:${enrichData.twitterDisplay || '无'} | img:${rawImage ? '有' : '无'} | ${wssLatency}ms`);

    // 标记 API 竞速已完成（TOKEN_EVENT 胜出）
    if (rawTwitter) {
      this._apiResolved.add(ca);
    };

    // ── 匿名 URL 处理：oEmbed + socialdata 并发竞速 ──────────────────────
    const tweetId = extractTweetId(rawTwitter);
    const isAnonymous = !!(rawTwitter && /\/(i)\/status\//i.test(rawTwitter) && !enrichData.twitterUsername);

    if (!apiAlreadyResolved) {
      if (isAnonymous && tweetId) {
        this._resolveAnonymousUrl(ca, tweetId);
      } else if (tweetId) {
        // 标准 URL：异步获取推文内容（不阻塞匹配，纯前端展示用）
        this._asyncFetchContent(ca, tweetId);
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  匿名 URL 并发解析（oEmbed 竞速 socialdata）
  // ═══════════════════════════════════════════════════════════════════════════

  _resolveAnonymousUrl(ca, tweetId) {
    let resolved = false;

    const applyScreenName = (screenName, source) => {
      if (resolved) return;
      resolved = true;

      const canonicalUrl = `https://x.com/${screenName}/status/${tweetId}`;
      // 通过 store.enrich 的 twitterUrlRewrite 触发条件①重评估
      this.store.enrich(ca, {
        twitterUrlRewrite: canonicalUrl,
        twitterHref: canonicalUrl,
        twitterDisplay: '@' + screenName,
        twitterUsername: screenName,
      });
      console.log(`[FourMeme] ✅ 账号解析(${source}) @${screenName} | CA:${ca.slice(0, 10)}...`);
    };

    // ① oEmbed（~500ms，免费）
    this._resolveViaOembed(tweetId).then(name => {
      if (name) applyScreenName(name, 'oEmbed');
    }).catch(() => {});

    // ② socialdata（~4s，同时拿推文内容）
    this._fetchTweetContent(tweetId).then(result => {
      if (!result) return;
      // 推文内容写入（前端展示）
      if (result.content && result.content.trim()) {
        this.store.enrich(ca, {
          twitterContent: result.content.trim(),
          twitterContentTime: result.time,
        });
      }
      // 账号兜底
      const screenName = result.user && result.user.screen_name;
      if (screenName && !resolved) {
        applyScreenName(screenName, 'socialdata');
      }
    }).catch(() => {});
  }

  /** 标准 URL 异步获取推文内容（纯展示，不影响匹配） */
  _asyncFetchContent(ca, tweetId) {
    this._fetchTweetContent(tweetId).then(result => {
      if (result && result.content && result.content.trim()) {
        this.store.enrich(ca, {
          twitterContent: result.content.trim(),
          twitterContentTime: result.time,
        });
      }
    }).catch(() => {});
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  外部 API 调用
  // ═══════════════════════════════════════════════════════════════════════════

  /** oEmbed 免费解析推文账号名（~500ms） */
  _resolveViaOembed(tweetId) {
    if (!tweetId) return Promise.resolve(null);
    return new Promise((resolve) => {
      const tweetUrl = encodeURIComponent(`https://x.com/i/status/${tweetId}`);
      const req = https.request({
        hostname: 'publish.twitter.com',
        path: `/oembed?url=${tweetUrl}&omit_script=true`,
        method: 'GET',
        headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' },
      }, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          try {
            if (res.statusCode !== 200) { resolve(null); return; }
            const json = JSON.parse(data);
            const m = (json.author_url || '').match(/(?:twitter|x)\.com\/([A-Za-z0-9_]{1,50})/i);
            const name = m ? m[1] : null;
            resolve(name && name.toLowerCase() !== 'i' ? name : null);
          } catch (_) { resolve(null); }
        });
      });
      req.setTimeout(4000, () => { req.destroy(); resolve(null); });
      req.on('error', () => resolve(null));
      req.end();
    });
  }

  /** socialdata API 获取推文内容 + 用户信息（~4s） */
  _fetchTweetContent(tweetId) {
    if (!tweetId) return Promise.resolve(null);

    // 缓存命中
    if (this._tweetCache.has(tweetId)) return Promise.resolve(this._tweetCache.get(tweetId));
    // 并发去重
    if (this._tweetFetching.has(tweetId)) return this._tweetFetching.get(tweetId);

    const promise = new Promise((resolve) => {
      const req = https.request({
        hostname: 'api.socialdata.tools',
        path: `/twitter/tweets/${tweetId}`,
        method: 'GET',
        headers: { 'Authorization': `Bearer ${this._apiKey}`, 'Accept': 'application/json' },
      }, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          this._tweetFetching.delete(tweetId);
          try {
            const json = JSON.parse(data);
            if (json.status === 'error') {
              console.warn(`[FourMeme] ⚠️ socialdata 错误: ${json.message || '未知'} | tweetId:${tweetId}`);
              resolve(null);
              return;
            }
            const content = json.full_text || json.text || null;
            const u = json.user || {};
            const user = u.id_str ? {
              name: u.name || '',
              screen_name: u.screen_name || '',
              profile_image: u.profile_image_url_https || '',
              verified: !!u.verified,
              followers: u.followers_count || 0,
            } : null;
            const result = {
              content,
              time: formatBeijingTimeMs(new Date()),
              user,
              retweet: json.retweet_count || 0,
              likes: json.favorite_count || 0,
              views: json.views_count || null,
            };
            if (content) this._tweetCache.set(tweetId, result);
            resolve(result);
          } catch (e) {
            console.warn(`[FourMeme] 推文解析失败: ${e.message}`);
            resolve(null);
          }
        });
      });
      req.on('error', (e) => {
        this._tweetFetching.delete(tweetId);
        console.error(`[FourMeme] 推文请求失败: ${e.message}`);
        resolve(null);
      });
      req.end();
    });

    this._tweetFetching.set(tweetId, promise);
    return promise;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  早到事件缓冲
  // ═══════════════════════════════════════════════════════════════════════════

  _bufferEvent(ca, data) {
    const now = Date.now();
    if (!this._earlyBuffer.has(ca)) this._earlyBuffer.set(ca, []);
    const bucket = this._earlyBuffer.get(ca);

    // 清理过期
    const valid = bucket.filter(e => now - e.arrivedAt < EARLY_BUFFER_TTL);
    while (valid.length >= EARLY_BUFFER_MAX) valid.shift();
    valid.push({ data, arrivedAt: now });
    this._earlyBuffer.set(ca, valid);
  }

  /** 当前连接状态 */
  isConnected() {
    return !!(this.ws && this.ws.readyState === WebSocket.OPEN);
  }
}

module.exports = FourMemeWatcher;
