'use strict';
/**
 * trader.js — 纯交易策略引擎
 *
 * 职责（单一）：
 *   - 接收匹配信号 → 立即买入（毫秒级）
 *   - 接收价格更新 → 三档止盈判定
 *   - 管理持仓状态 + 交易历史
 *
 * 不负责：
 *   - 不负责数据查询（由 store 提供）
 *   - 不负责前端推送（由 index.js 监听 trader 事件）
 *   - 不负责持久化调度（由 index.js 在 trade 事件后调用 storage）
 *
 * 三档止盈策略（对当前余额的比例）：
 *   MC ≥ 30K  → 卖 50%（剩余 50%）
 *   MC ≥ 40K  → 卖 50%（剩余 25%，对当前是50%）
 *   MC ≥ 100K → 卖 100%（清仓）
 */

const { EventEmitter } = require('events');

class TradingEngine extends EventEmitter {
  /**
   * @param {object} config - 交易配置
   * @param {object} blockchainService - 链上交互服务
   * @param {object} storage - 持久化服务（可选）
   */
  constructor(config, blockchainService, storage) {
    super();
    this.config = config;
    this.chain = blockchainService;
    this.storage = storage || null;

    this.positions = new Map();    // tokenAddress → position
    this.tradeHistory = [];
    this._processing = new Set();  // 防并发锁: tokenId / tokenId_sellN
    this.bnbPriceUSD = config.fixedBNBPrice || 580;

    // 链上创建事件策略止盈配置
    this.createSellThresholdUSD = config.createSellThresholdUSD || 15000;
    this.createSellRatio = config.createSellRatio || 1.0;

    // 从持久化恢复
    this._restore();
  }

  _restore() {
    if (!this.storage) return;
    const s = this.storage.state;
    if (Array.isArray(s.tradeHistory)) {
      this.tradeHistory = s.tradeHistory.slice(0, 500);
    }
    if (Array.isArray(s.positions)) {
      for (const p of s.positions) {
        if (p && p.tokenAddress) this.positions.set(p.tokenAddress, p);
      }
      console.log(`[Trader] ♻️ 恢复 ${this.positions.size} 个持仓 / ${this.tradeHistory.length} 条记录`);
    }
  }

  _persist() {
    if (!this.storage) return;
    this.storage.update({
      positions: Array.from(this.positions.values()),
      tradeHistory: this.tradeHistory.slice(0, 500),
    });
  }

  setBNBPrice(price) { if (price > 0) this.bnbPriceUSD = price; }

  // ═══════════════════════════════════════════════════════════════════════════
  //  匹配成功 → 瞬间买入（由 store._tryMatch 同步直调）
  // ═══════════════════════════════════════════════════════════════════════════

  async onMatched(token) {
    const tokenAddress = token.tokenAddress || token.address;
    const tokenId = token.tokenId;
    if (!tokenAddress || !tokenId) return;

    // 已持仓或正在处理 → 跳过
    if (this.positions.has(tokenAddress)) return;
    if (this._processing.has(tokenId)) return;
    this._processing.add(tokenId);

    try {
      console.log(`[Trader] 🚀 买入 ${token.symbol || token.name} | ${tokenAddress.slice(0, 10)}... | ${this.config.buyAmountBNB} BNB`);

      // buyToken 立即返回 hash（不等确认，毫秒级）
      const result = await this.chain.buyToken(tokenAddress, this.config.buyAmountBNB, (confirmed) => {
        // 链上确认回填（异步，不阻塞）
        const pos = this.positions.get(tokenAddress);
        if (pos) {
          if (confirmed.tokenReceived) pos.tokenReceived = confirmed.tokenReceived;
          if (confirmed.blockNumber) pos.buyBlock = confirmed.blockNumber;
          pos.buyConfirmed = confirmed.status === 1;
          this._persist();
          this.emit('trade_update', {
            txHash: confirmed.txHash,
            tokenReceived: pos.tokenReceived,
            blockNumber: confirmed.blockNumber,
            status: confirmed.status,
          });
        }
      });

      const trade = {
        type: 'BUY',
        tokenId,
        tokenAddress,
        symbol: token.symbol,
        name: token.name || token.fmName,
        image: token.image,
        time: new Date().toLocaleString('zh-CN', { hour12: false }),
        bnbSpent: this.config.buyAmountBNB,
        tokenReceived: '0',
        txHash: result.txHash,
        success: result.success,
        error: result.error || null,
        simulated: result.simulated || false,
        marketCapAtBuy: token.marketCapUSD || 0,
        pending: !!result.pending,
        matchReason: token.matchReason || '',
      };

      this.tradeHistory.unshift(trade);
      if (this.tradeHistory.length > 500) this.tradeHistory.length = 500;

      if (result.success) {
        this.positions.set(tokenAddress, {
          tokenId,
          tokenAddress,
          symbol: token.symbol,
          name: token.name || token.fmName,
          image: token.image,
          buyTime: new Date().toISOString(),
          bnbSpent: this.config.buyAmountBNB,
          tokenReceived: '0',
          buyTxHash: result.txHash,
          buyConfirmed: false,
          sold1: false, sold2: false, sold3: false,
          currentMarketCap: 0,
          marketCapAtBuy: token.marketCapUSD || 0,
        });
        token.bought = true;
        token.txBuy = result.txHash;
        token.buyStatus = 'pending';
      } else {
        token.buyStatus = 'failed';
        console.error(`[Trader] ❌ 买入失败: ${result.error}`);
      }

      this.emit('trade', trade);
      this._persist();
    } catch (err) {
      token.buyStatus = 'failed';
      console.error('[Trader] 买入异常:', err.message);
    } finally {
      this._processing.delete(tokenId);
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  链上创建事件策略匹配 → 毫秒级买入（由 store._tryCreateMatch 同步直调）
  // ═══════════════════════════════════════════════════════════════════════════

  async onCreateMatched(token) {
    const tokenAddress = token.tokenAddress || token.address;
    const tokenId = token.tokenId;
    if (!tokenAddress || !tokenId) return;

    // 已持仓或正在处理 → 跳过
    if (this.positions.has(tokenAddress)) return;
    if (this._processing.has(tokenId)) return;
    this._processing.add(tokenId);

    try {
      console.log(`[Trader] 🚀🏗️ 链上创建策略买入 ${token.symbol || token.name} | ${tokenAddress.slice(0, 10)}... | ${this.config.buyAmountBNB} BNB | ${token._createMatchReason}`);

      const result = await this.chain.buyToken(tokenAddress, this.config.buyAmountBNB, (confirmed) => {
        const pos = this.positions.get(tokenAddress);
        if (pos) {
          if (confirmed.tokenReceived) pos.tokenReceived = confirmed.tokenReceived;
          if (confirmed.blockNumber) pos.buyBlock = confirmed.blockNumber;
          pos.buyConfirmed = confirmed.status === 1;
          this._persist();
          this.emit('trade_update', {
            txHash: confirmed.txHash,
            tokenReceived: pos.tokenReceived,
            blockNumber: confirmed.blockNumber,
            status: confirmed.status,
          });
        }
      });

      const trade = {
        type: 'BUY',
        strategy: 'create',  // 标记来源策略
        tokenId,
        tokenAddress,
        symbol: token.symbol,
        name: token.name || token.fmName,
        image: token.image,
        time: new Date().toLocaleString('zh-CN', { hour12: false }),
        bnbSpent: this.config.buyAmountBNB,
        tokenReceived: '0',
        txHash: result.txHash,
        success: result.success,
        error: result.error || null,
        simulated: result.simulated || false,
        marketCapAtBuy: token.marketCapUSD || 0,
        pending: !!result.pending,
        matchReason: token._createMatchReason,
      };

      this.tradeHistory.unshift(trade);
      if (this.tradeHistory.length > 500) this.tradeHistory.length = 500;

      if (result.success) {
        this.positions.set(tokenAddress, {
          tokenId,
          tokenAddress,
          symbol: token.symbol,
          name: token.name || token.fmName,
          image: token.image,
          strategy: 'create',  // 标记：链上创建策略持仓
          buyTime: new Date().toISOString(),
          bnbSpent: this.config.buyAmountBNB,
          tokenReceived: '0',
          buyTxHash: result.txHash,
          buyConfirmed: false,
          sold1: false, sold2: false, sold3: false,
          currentMarketCap: 0,
          marketCapAtBuy: token.marketCapUSD || 0,
        });
        token.bought = true;
        token.txBuy = result.txHash;
        token.buyStatus = 'pending';
      } else {
        token.buyStatus = 'failed';
        console.error(`[Trader] ❌ 链上创建策略买入失败: ${result.error}`);
      }

      this.emit('trade', trade);
      this._persist();
    } catch (err) {
      token.buyStatus = 'failed';
      console.error('[Trader] 链上创建策略买入异常:', err.message);
    } finally {
      this._processing.delete(tokenId);
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  价格更新 → 三档止盈（由 index.js 在 store price_updated 事件后调用）
  // ═══════════════════════════════════════════════════════════════════════════

  async onPriceUpdate(tokenAddress, marketCapUSD, token) {
    if (!tokenAddress) return;
    const position = this.positions.get(tokenAddress);
    if (!position) return;

    position.currentMarketCap = marketCapUSD;

    // ── 链上创建策略持仓：市值 15K 全卖 ──────────────────────────────────
    if (position.strategy === 'create') {
      if (!position.sold1 && marketCapUSD >= this.createSellThresholdUSD) {
        await this._sellTier(tokenAddress, token, position, 1, this.createSellRatio, marketCapUSD);
      }
      return;
    }

    // ── 原四条件策略：三档止盈 ────────────────────────────────────────────
    if (!position.sold1 && marketCapUSD >= this.config.sellThreshold1USD) {
      await this._sellTier(tokenAddress, token, position, 1, this.config.sellRatio1, marketCapUSD);
    } else if (position.sold1 && !position.sold2 && marketCapUSD >= this.config.sellThreshold2USD) {
      await this._sellTier(tokenAddress, token, position, 2, this.config.sellRatio2, marketCapUSD);
    } else if (position.sold2 && !position.sold3 && marketCapUSD >= this.config.sellThreshold3USD) {
      await this._sellTier(tokenAddress, token, position, 3, this.config.sellRatio3, marketCapUSD);
    }
  }

  async _sellTier(tokenAddress, token, position, tier, ratio, marketCapUSD) {
    const tokenId = position.tokenId || tokenAddress;
    const lockKey = `${tokenId}_sell${tier}`;
    if (this._processing.has(lockKey)) return;
    this._processing.add(lockKey);

    try {
      console.log(`[Trader] 📈 第${tier}档 | MC:$${Math.round(marketCapUSD).toLocaleString()} | 卖 ${(ratio * 100).toFixed(0)}%`);

      const result = await this.chain.sellToken(tokenAddress, ratio, (confirmed) => {
        this.emit('trade_update', {
          txHash: confirmed.txHash,
          bnbReceived: confirmed.bnbReceived,
          status: confirmed.status,
        });
        this._persist();
      });

      const trade = {
        type: `SELL_${tier}`,
        tokenId,
        tokenAddress,
        symbol: token?.symbol || position.symbol,
        name: position.name,
        image: position.image,
        time: new Date().toLocaleString('zh-CN', { hour12: false }),
        ratio,
        bnbReceived: result.bnbReceived || '0',
        soldAmount: result.soldAmount || '0',
        txHash: result.txHash || null,
        success: result.success,
        error: result.error || null,
        simulated: result.simulated || false,
        marketCapAtSell: marketCapUSD,
        pending: !!result.pending,
      };

      this.tradeHistory.unshift(trade);
      if (this.tradeHistory.length > 500) this.tradeHistory.length = 500;

      if (result.success) {
        position[`sold${tier}`] = true;
        if (token) {
          token[`sold${tier}`] = true;
          token[`txSell${tier}`] = result.txHash;
        }
        // 第三档清仓
        if (tier === 3) this.positions.delete(tokenAddress);
      }

      this.emit('trade', trade);
      this._persist();
    } catch (err) {
      console.error(`[Trader] 第${tier}档异常:`, err.message);
    } finally {
      this._processing.delete(lockKey);
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  查询接口
  // ═══════════════════════════════════════════════════════════════════════════

  getPositions() { return Array.from(this.positions.values()); }
  getTradeHistory() { return this.tradeHistory; }
  hasPosition(tokenAddress) { return this.positions.has(tokenAddress); }
}

module.exports = TradingEngine;
