'use strict';
/**
 * storage.js — 持久化层
 *
 * 设计原则：第一性原理 + 一处获取同步更新
 *   - 一个 CA 一个文件（data/memes/{ca}.json）
 *   - 文件内容 = MEME元数据 + 交易明细 + 市值历史
 *   - 优先顺序：交易逻辑匹配 > 前端显示 > 文件写入
 *   - 文件写入全部异步 + 去抖，不阻塞任何热路径
 *
 * 文件结构：
 *   data/state.json        — 持仓/交易历史/钱包监控等状态
 *   data/events.ndjson     — 全量事件日志（追加式）
 *   data/memes/{ca}.json   — 每个MEME独立文件
 */

const fs = require('fs');
const path = require('path');

const DATA_DIR    = path.join(__dirname, '..', 'data');
const STATE_FILE  = path.join(DATA_DIR, 'state.json');
const EVENTS_FILE = path.join(DATA_DIR, 'events.ndjson');
const MEMES_DIR   = path.join(DATA_DIR, 'memes');

const STATE_DEBOUNCE_MS = 2000;
const MEME_DEBOUNCE_MS  = 3000;  // 单个MEME文件写入去抖

class Storage {
  constructor() {
    try {
      if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
      if (!fs.existsSync(MEMES_DIR)) fs.mkdirSync(MEMES_DIR, { recursive: true });
    } catch (e) {
      console.warn('[Storage] 创建目录失败:', e.message);
    }
    this._state = this._loadState();
    this._stateDirty = false;
    this._stateTimer = null;
    this._stateWriting = false;

    // MEME 文件去抖：ca -> timer
    this._memeTimers = new Map();
    this._memeWriting = new Set();
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  State（持仓/交易历史/钱包监控）
  // ═══════════════════════════════════════════════════════════════════════════

  _loadState() {
    try {
      if (fs.existsSync(STATE_FILE)) {
        const parsed = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
        return {
          positions:       Array.isArray(parsed.positions)       ? parsed.positions       : [],
          tradeHistory:    Array.isArray(parsed.tradeHistory)    ? parsed.tradeHistory    : [],
          walletTxHistory: Array.isArray(parsed.walletTxHistory) ? parsed.walletTxHistory : [],
          tokens:          Array.isArray(parsed.tokens)          ? parsed.tokens          : [],
        };
      }
    } catch (e) {
      console.warn('[Storage] state.json 读取失败:', e.message);
    }
    return { positions: [], tradeHistory: [], walletTxHistory: [], tokens: [] };
  }

  get state() { return this._state; }

  update(patch) {
    this._state = { ...this._state, ...patch };
    this._stateDirty = true;
    if (this._stateTimer) return;
    this._stateTimer = setTimeout(() => {
      this._stateTimer = null;
      this._flushState();
    }, STATE_DEBOUNCE_MS);
  }

  _flushState() {
    if (!this._stateDirty || this._stateWriting) return;
    this._stateDirty = false;
    this._stateWriting = true;
    const tmp = STATE_FILE + '.tmp';
    fs.writeFile(tmp, JSON.stringify(this._state), (err) => {
      if (err) { this._stateWriting = false; return; }
      fs.rename(tmp, STATE_FILE, () => { this._stateWriting = false; });
    });
  }

  flushSync() {
    if (this._stateDirty) {
      try { fs.writeFileSync(STATE_FILE, JSON.stringify(this._state)); }
      catch (e) { console.error('[Storage] state flush失败:', e.message); }
    }
  }

  appendEvent(type, data) {
    try {
      const line = JSON.stringify({ ts: Date.now(), type, data }) + '\n';
      fs.appendFile(EVENTS_FILE, line, () => {});
    } catch (_) {}
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  MEME 持久化：一个CA一个文件
  //  文件内容：{ meta: {...}, trades: [...], mcHistory: [...] }
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * 写入/更新 MEME 文件（去抖，异步）
   * @param {object} token - store 中的 token 对象
   */
  persistMeme(token) {
    if (!token || !token.tokenId) return;
    // 只持久化被狙击的MEME（已买入 或 已匹配）
    if (!token.bought && !token.matchReason && !token._createStrategyMatched) return;
    const ca = token.tokenId.toLowerCase();

    // 去抖：同一 CA 多次写入合并
    if (this._memeTimers.has(ca)) return;
    this._memeTimers.set(ca, setTimeout(() => {
      this._memeTimers.delete(ca);
      this._writeMemeFile(ca, token);
    }, MEME_DEBOUNCE_MS));
  }

  /**
   * 追加交易到 MEME 文件（去抖写入，交易先存到 token.trades 内存中）
   * 实际写入由 persistMeme 统一处理
   */
  persistTrade(token) {
    this.persistMeme(token);
  }

  /**
   * 实际写入 MEME 文件
   */
  _writeMemeFile(ca, token) {
    if (this._memeWriting.has(ca)) return;
    this._memeWriting.add(ca);

    const filePath = path.join(MEMES_DIR, `${ca}.json`);
    const tmp = filePath + '.tmp';

    const payload = {
      meta: {
        tokenId: token.tokenId,
        tokenAddress: token.tokenAddress,
        symbol: token.symbol,
        name: token.name,
        fmSymbol: token.fmSymbol,
        fmName: token.fmName,
        image: token.image,
        twitterUrl: token.twitterUrl,
        twitterDisplay: token.twitterDisplay,
        twitterHref: token.twitterHref,
        twitterUsername: token.twitterUsername,
        twitterCreatedAt: token.twitterCreatedAt,
        twitterContent: token.twitterContent,
        programGetTime: token.programGetTime,
        mediaAddressTime: token.mediaAddressTime,
        twitterContentTime: token.twitterContentTime,
        marketCapUSD: token.marketCapUSD,
        mediaSource: token.mediaSource,
        mediaLatencyMs: token.mediaLatencyMs,
        mediaMatched: token.mediaMatched,
        mediaTimeMatched: token.mediaTimeMatched,
        walletSignals: (token.walletSignals || []).slice(0, 100),
        bought: token.bought,
        sold1: token.sold1,
        sold2: token.sold2,
        matchReason: token.matchReason,
        buyStatus: token.buyStatus,
        arrivalTime: token.arrivalTime,
        tamount: token.tamount,
      },
      // 最近200条交易
      trades: (token.trades || []).slice(-200).map(t => ({
        side: t.side,
        userAddress: t.userAddress,
        bnbAmount: t.bnbAmount,
        tokenAmount: t.tokenAmount,
        txHash: t.txHash,
        time: t.time,
        marketCapUSD: t.marketCapUSD || 0,
      })),
    };

    fs.writeFile(tmp, JSON.stringify(payload), (err) => {
      if (err) { this._memeWriting.delete(ca); return; }
      fs.rename(tmp, filePath, () => { this._memeWriting.delete(ca); });
    });
  }

  /**
   * 加载所有持久化的 MEME（启动时调用）
   * @returns {Array} token 对象列表
   */
  loadAllMemes() {
    const tokens = [];
    try {
      const files = fs.readdirSync(MEMES_DIR).filter(f => f.endsWith('.json'));
      for (const file of files) {
        try {
          const raw = fs.readFileSync(path.join(MEMES_DIR, file), 'utf8');
          const data = JSON.parse(raw);
          if (data && data.meta && data.meta.tokenId) {
            tokens.push({
              ...data.meta,
              trades: data.trades || [],
            });
          }
        } catch (_) {}
      }
    } catch (e) {
      console.warn('[Storage] MEME目录读取失败:', e.message);
    }
    console.log(`[Storage] ♻️ 恢复 ${tokens.length} 个持久化MEME`);
    return tokens;
  }
}

module.exports = Storage;
