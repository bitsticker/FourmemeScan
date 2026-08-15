'use strict';
/**
 * blockchain.js
 * Four.meme 自动交易机器人 - BlockchainService
 *
 * 核心优化：
 *   1. buyToken：提交 tx 后立即返回 hash，链上确认 + 余额查询均异步回填
 *      （主流程 0ms 阻塞，不影响下一次毫秒级触发）
 *   2. sellToken：完整实现（approve 一次性 MAX，后续调用 proxy.sell）
 *   3. GAS：动态最小值（config.gasPriceGwei，默认 3 Gwei），保证快速打包
 *   4. nonce 本地管理，避免 RPC 往返
 */

const { ethers } = require('ethers');

const PROXY_ABI = [
  { "inputs": [{ "name": "token", "type": "address" }, { "name": "to", "type": "address" }, { "name": "funds", "type": "uint256" }, { "name": "minAmount", "type": "uint256" }], "name": "buyTokenAMAP", "outputs": [], "stateMutability": "payable", "type": "function" },
  { "inputs": [{ "name": "token", "type": "address" }, { "name": "ethAmount", "type": "uint256" }], "name": "getAmountOut", "outputs": [{ "type": "uint256" }], "stateMutability": "view", "type": "function" },
  { "inputs": [{ "name": "token", "type": "address" }, { "name": "amount", "type": "uint256" }, { "name": "minEth", "type": "uint256" }], "name": "sell", "outputs": [], "stateMutability": "nonpayable", "type": "function" }
];

const ERC20_ABI = [
  { "inputs": [{ "name": "account", "type": "address" }], "name": "balanceOf", "outputs": [{ "type": "uint256" }], "stateMutability": "view", "type": "function" },
  { "inputs": [{ "name": "spender", "type": "address" }, { "name": "amount", "type": "uint256" }], "name": "approve", "outputs": [{ "type": "bool" }], "stateMutability": "nonpayable", "type": "function" },
  { "inputs": [{ "name": "owner", "type": "address" }, { "name": "spender", "type": "address" }], "name": "allowance", "outputs": [{ "type": "uint256" }], "stateMutability": "view", "type": "function" }
];

const PROXY_ADDRESS = '0x5c952063c7fc8610ffdb798152d69f0b9550762b';
const MAX_UINT256  = (1n << 256n) - 1n;

class BlockchainService {
  constructor(config) {
    this.config   = config;
    this.provider = null;
    this.wallet   = null;
    this.proxy    = null;
    this.initialized = false;

    // 本地 nonce 管理（避免每笔交易都向 RPC 询问）
    this._nonce = null;

    // 已授权代币缓存（一次 approve MAX，后续零开销）
    this._approved = new Set();

    // GAS 配置
    this._gasPrice = null;          // BigInt
    this._buyGasLimit = BigInt(config.buyGasLimit || 300000);
    this._sellGasLimit = BigInt(config.sellGasLimit || 400000);
    this._approveGasLimit = BigInt(config.approveGasLimit || 80000);
  }

  async init() {
    try {
      // 使用配置的 RPC URL
      const rpcUrl = this.config.bscRpcUrl || 'https://bsc-mainnet.nodereal.io/v1/9c4a207b2cb541d1a81257ffaa5fbd92';
      const provider = new ethers.JsonRpcProvider(rpcUrl);
      const network = await Promise.race([
        provider.getNetwork(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('RPC 超时(5s)')), 5000)),
      ]);
      if (network.chainId !== 56n) {
        throw new Error(`链 ID 错误: ${network.chainId}`);
      }
      this.provider = provider;
      console.log(`[BlockchainService] ✅ RPC 连接成功: ${rpcUrl}`);
      if (!this.config.privateKey || this.config.privateKey === 'your_private_key_here') {
        console.warn('[BlockchainService] ⚠️ 未配置私钥，仅模拟模式');
        this.initialized = false;
        return false;
      }

      const pk = this.config.privateKey.startsWith('0x') ? this.config.privateKey : '0x' + this.config.privateKey;
      this.wallet = new ethers.Wallet(pk, this.provider);
      this.proxy  = new ethers.Contract(PROXY_ADDRESS, PROXY_ABI, this.wallet);

      // 本地 nonce 初始化（pending，包含已提交未确认的交易）
      this._nonce = await this.provider.getTransactionCount(this.wallet.address, 'pending');

      // GAS 价格固化（BSC 相对稳定；如需动态可定期刷新）
      const gwei = this.config.gasPriceGwei || 3;
      this._gasPrice = ethers.parseUnits(String(gwei), 'gwei');

      // 余额非阻塞查询（异步打印，不卡初始化）
      this.provider.getBalance(this.wallet.address).then(b => {
        console.log(`[BlockchainService] 💰 BNB 余额: ${ethers.formatEther(b)} BNB`);
      }).catch(() => {});

      console.log(`[BlockchainService] ✅ 已连接 BSC，钱包: ${this.wallet.address}`);
      console.log(`[BlockchainService] ⚡ GAS: ${gwei} Gwei | nonce: ${this._nonce}`);

      this.initialized = true;
      return true;
    } catch (err) {
      console.error('[BlockchainService] 初始化失败:', err.message);
      this.initialized = false;
      return false;
    }
  }

  _nextNonce() {
    const n = this._nonce;
    this._nonce += 1;
    return n;
  }

  /**
   * 买入 —— 毫秒级路径
   *
   * 流程：
   *   1. 同步构造 tx 参数并发送（约 20~80ms RPC RTT）
   *   2. 立即返回 { success:true, txHash }，不等 receipt
   *   3. 后台异步 tx.wait + balanceOf，完成后通过 onConfirmed 回调回填
   *
   * @param {string} tokenAddress
   * @param {number|string} bnbAmount
   * @param {(confirmed)=>void} onConfirmed  链上确认后回调（含 tokenReceived / gasUsed）
   */
  async buyToken(tokenAddress, bnbAmount, onConfirmed) {
    if (!this.initialized || !this.proxy) {
      return { success: false, error: '区块链服务未初始化', simulated: true };
    }

    try {
      const valueWei = ethers.parseEther(bnbAmount.toString());
      const nonce    = this._nextNonce();

      console.log(`[Buy] 🛒 buyTokenAMAP ${tokenAddress} | ${bnbAmount} BNB | nonce:${nonce}`);

      const tx = await this.proxy.buyTokenAMAP(
        tokenAddress,
        this.wallet.address,
        valueWei,
        0n,
        {
          value:    valueWei,
          gasLimit: this._buyGasLimit,
          gasPrice: this._gasPrice,
          nonce,
        }
      );

      console.log(`[Buy] ✅ 交易已发送: ${tx.hash}`);

      // ── 后台确认 + 余额查询（不阻塞主流程）──────────────────────────────
      tx.wait(1).then(async (receipt) => {
        let tokenReceived = '0';
        try {
          const erc20 = new ethers.Contract(tokenAddress, ERC20_ABI, this.provider);
          const bal   = await erc20.balanceOf(this.wallet.address);
          tokenReceived = ethers.formatUnits(bal, 18);
        } catch (_) {}
        console.log(`[Buy] ✅ 确认 block:${receipt.blockNumber} gas:${receipt.gasUsed}`);
        if (typeof onConfirmed === 'function') {
          onConfirmed({ txHash: tx.hash, blockNumber: receipt.blockNumber, gasUsed: receipt.gasUsed.toString(), tokenReceived, status: receipt.status });
        }
      }).catch((err) => {
        console.error('[Buy] 等待确认失败:', err.message);
        if (typeof onConfirmed === 'function') {
          onConfirmed({ txHash: tx.hash, error: err.message, status: 0 });
        }
      });

      // ── 立即返回（不等 receipt）─────────────────────────────────────────
      return { success: true, txHash: tx.hash, pending: true };
    } catch (err) {
      console.error('[Buy] ❌ 购买失败:', err.message);
      // nonce 发送失败时回退，避免错号
      if (this._nonce != null) this._nonce -= 1;
      return { success: false, error: err.message || '未知错误' };
    }
  }

  /**
   * 卖出 —— 对当前持仓余额的 ratio 比例卖出
   *
   * 流程：
   *   1. 读取当前代币余额（balanceOf）
   *   2. 首次卖出前对 PROXY 授权 MAX（仅一次）
   *   3. 调用 proxy.sell(token, amount, 0)
   *
   * @param {string} tokenAddress
   * @param {number} ratio    0~1
   * @param {(confirmed)=>void} onConfirmed
   */
  async sellToken(tokenAddress, ratio = 1.0, onConfirmed) {
    if (!this.initialized || !this.proxy) {
      return { success: false, error: '区块链服务未初始化', simulated: true };
    }

    try {
      const erc20 = new ethers.Contract(tokenAddress, ERC20_ABI, this.wallet);
      const bal   = await erc20.balanceOf(this.wallet.address);
      if (bal === 0n) {
        return { success: false, error: '代币余额为 0，无法出售' };
      }

      // 计算卖出数量（按当前余额 × ratio，整数 BigInt 运算）
      // ratio 精度放大 10000 倍避免浮点
      const r = Math.max(0, Math.min(1, ratio));
      const scale = 10000n;
      const rScaled = BigInt(Math.round(r * 10000));
      const amount  = (bal * rScaled) / scale;
      if (amount === 0n) {
        return { success: false, error: '出售数量为 0' };
      }

      // ── approve MAX（每个 token 只做一次）─────────────────────────────
      const tokenKey = tokenAddress.toLowerCase();
      if (!this._approved.has(tokenKey)) {
        try {
          const allowance = await erc20.allowance(this.wallet.address, PROXY_ADDRESS);
          if (allowance < amount) {
            const approveTx = await erc20.approve(PROXY_ADDRESS, MAX_UINT256, {
              gasLimit: this._approveGasLimit,
              gasPrice: this._gasPrice,
              nonce:    this._nextNonce(),
            });
            console.log(`[Sell] 🔐 approve MAX: ${approveTx.hash}`);
            await approveTx.wait(1);  // 必须等待 approve 确认，否则 sell 会 revert
          }
          this._approved.add(tokenKey);
        } catch (e) {
          console.warn('[Sell] approve 失败:', e.message);
          return { success: false, error: 'approve 失败: ' + e.message };
        }
      }

      const nonce = this._nextNonce();
      const tx = await this.proxy.sell(tokenAddress, amount, 0n, {
        gasLimit: this._sellGasLimit,
        gasPrice: this._gasPrice,
        nonce,
      });

      const soldAmountStr = ethers.formatUnits(amount, 18);
      console.log(`[Sell] ✅ 交易已发送: ${tx.hash} | ratio:${(r*100).toFixed(1)}% | 数量:${soldAmountStr}`);

      // 后台确认，主流程立即返回
      tx.wait(1).then((receipt) => {
        // 估算收到的 BNB：扫描 receipt.logs 里发给自己的 WBNB Transfer（非强制）
        const bnbReceived = this._estimateBnbFromReceipt(receipt, this.wallet.address);
        console.log(`[Sell] ✅ 确认 block:${receipt.blockNumber} gas:${receipt.gasUsed} bnb:${bnbReceived}`);
        if (typeof onConfirmed === 'function') {
          onConfirmed({ txHash: tx.hash, blockNumber: receipt.blockNumber, gasUsed: receipt.gasUsed.toString(), bnbReceived, status: receipt.status });
        }
      }).catch((err) => {
        console.error('[Sell] 等待确认失败:', err.message);
        if (typeof onConfirmed === 'function') {
          onConfirmed({ txHash: tx.hash, error: err.message, status: 0 });
        }
      });

      return { success: true, txHash: tx.hash, soldAmount: soldAmountStr, pending: true };
    } catch (err) {
      console.error('[Sell] ❌ 出售失败:', err.message);
      if (this._nonce != null) this._nonce -= 1;
      return { success: false, error: err.message || '未知错误' };
    }
  }

  /**
   * 从 receipt 中估算接收到的 BNB（扫描 WBNB Transfer）
   */
  _estimateBnbFromReceipt(receipt, walletAddr) {
    try {
      if (!receipt || !Array.isArray(receipt.logs) || !walletAddr) return '0';
      const WBNB = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c';
      const TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
      const walletTopic = ('0x000000000000000000000000' + walletAddr.slice(2)).toLowerCase();
      let sum = 0n;
      for (const lg of receipt.logs) {
        if ((lg.address || '').toLowerCase() !== WBNB) continue;
        if (!lg.topics || lg.topics[0].toLowerCase() !== TRANSFER_TOPIC) continue;
        if (lg.topics[2] && lg.topics[2].toLowerCase() === walletTopic) {
          try { sum += BigInt(lg.data); } catch (_) {}
        }
      }
      return sum > 0n ? ethers.formatEther(sum) : '0';
    } catch (_) {
      return '0';
    }
  }

  async getTokenBalance(tokenAddress) {
    if (!this.wallet) return '0';
    try {
      const erc20 = new ethers.Contract(tokenAddress, ERC20_ABI, this.provider);
      const bal   = await erc20.balanceOf(this.wallet.address);
      return ethers.formatUnits(bal, 18);
    } catch (_) {
      return '0';
    }
  }

  async getBNBBalance() {
    if (!this.provider || !this.wallet) return '0';
    try {
      const bal = await this.provider.getBalance(this.wallet.address);
      return ethers.formatEther(bal);
    } catch (_) {
      return '0';
    }
  }

  getWalletAddress() {
    return this.wallet ? this.wallet.address : null;
  }
}

module.exports = BlockchainService;
