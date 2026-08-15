'use strict';
/**
 * config.js — 统一配置文件
 *
 * WSS 架构（重构后）：
 *   WSS-1: 只负责 TOKEN_CREATE，配置 3 条备用 RPC，断开自动切换到下一条
 *   WSS-2: 只负责 TOKEN_BUY + TOKEN_SELL（高流量，合并）
 *   WSS-4: four.meme WSS（独立，保持不变）
 *   WSS-3: 已删除
 */

// ─── 网络节点 ─────────────────────────────────────────────────────────────────
const NETWORK = {
  // ── WSS-1：TOKEN_CREATE 专用，3 条备用 RPC（断线自动轮换） ──────────────
  // 按优先级排列，第一条断开后自动切换到第二条，以此类推，循环复用
  bscWssCreatePool: [
    process.env.BSC_WSS_CREATE_1 || 'wss://shared.ap-southeast-1.getblock.io/17ae717b54404b07b5ae778465e2c4a1',
    process.env.BSC_WSS_CREATE_2 || 'wss://bsc-rpc.publicnode.com',
    process.env.BSC_WSS_CREATE_3 || 'wss://bsc-mainnet.nodereal.io/ws/v1/9c4a207b2cb541d1a81257ffaa5fbd92',
  ],

  // ── WSS-2：TOKEN_BUY + TOKEN_SELL（高流量，独占） ────────────────────────
  bscWssBuy: process.env.BSC_WSS_BUY || 'wss://bsc-rpc.publicnode.com',

  // HTTP RPC（发交易/查余额专用，独立于 WSS）
  bscRpcUrl: process.env.BSC_RPC_URL || 'https://bsc-mainnet.nodereal.io/v1/9c4a207b2cb541d1a81257ffaa5fbd92',

  // four.meme WSS-4（保持不变）
  fourMemeWss: process.env.FOUR_MEME_WSS || 'wss://ws.four.meme/ws',
  fourMemeApi: process.env.FOUR_MEME_API || 'https://four.meme/meme-api/v1/private/token/get/v2?address=',
};

// ─── 第三方 API ───────────────────────────────────────────────────────────────
const API = {
  socialDataKey: process.env.SOCIAL_DATA_KEY || '8032|hifLSMfrnto8XRGHwaMnfjl4bMzvB42znjoJTqqy9aa7f8bd',
};

// ─── 交易参数 ─────────────────────────────────────────────────────────────────
const TRADE = {
  privateKey:          process.env.PRIVATE_KEY          || '0x2249cd84b93fccf22ff60d5e1feee595bf45646494b03c67a2e2e922718d0f50',
  port:                parseInt(process.env.PORT         || '3001'),

  buyAmountBNB:        parseFloat(process.env.BUY_AMOUNT_BNB          || '0.01'),

  sellThreshold1USD:   parseFloat(process.env.SELL_THRESHOLD_1_USD    || '30000'),
  sellRatio1:          parseFloat(process.env.SELL_RATIO_1            || '0.5'),
  sellThreshold2USD:   parseFloat(process.env.SELL_THRESHOLD_2_USD    || '40000'),
  sellRatio2:          parseFloat(process.env.SELL_RATIO_2            || '0.5'),
  sellThreshold3USD:   parseFloat(process.env.SELL_THRESHOLD_3_USD    || '100000'),
  sellRatio3:          parseFloat(process.env.SELL_RATIO_3            || '1.0'),

  // ── 策略二：链上创建事件（关键词 + 创建者买入 → 毫秒级买入，15K全卖）────
  // 关键词匹配 ticker/name，CREATE 事件到达后立即匹配，匹配则直接执行买卖
  createKeywords: (process.env.CREATE_KEYWORDS || '首,first,最,most,新,new,爆火,热搜,第一,官,official,币,coin,meme,吉祥物,币安,安,binance,nance,何一,赵长鹏,表格,CZ,何,一,1,he,yi,4,four,四,BNB,赵,长,鹏').split(',').map(s => s.trim().toLowerCase()).filter(Boolean),
  createBuyMinBNB:     parseFloat(process.env.CREATE_BUY_MIN_BNB      || '0.03'),
  createBuyMaxBNB:     parseFloat(process.env.CREATE_BUY_MAX_BNB      || '2'),
  createSellThresholdUSD: parseFloat(process.env.CREATE_SELL_THRESHOLD_USD || '15000'),
  createSellRatio:     parseFloat(process.env.CREATE_SELL_RATIO       || '1.0'),

  // 媒体匹配时间窗口（分钟）— 策略一
  timeWindowMinutes:   parseFloat(process.env.TIME_WINDOW_MINUTES     || '3'),

  fixedBNBPrice:       parseFloat(process.env.FIXED_BNB_PRICE         || '0'),

  gasPriceGwei:        parseFloat(process.env.GAS_PRICE_GWEI          || '3'),
  buyGasLimit:         parseInt(process.env.BUY_GAS_LIMIT             || '300000'),
  sellGasLimit:        parseInt(process.env.SELL_GAS_LIMIT            || '400000'),
  approveGasLimit:     parseInt(process.env.APPROVE_GAS_LIMIT         || '80000'),

  targetAccounts: (process.env.TARGET_ACCOUNTS || '/cz_binance/,/heyibinance/,/binance/,/binancezh/,/binancewallet/,/binanceacademy/')
    .split(',').map(s => s.trim()),
};

// ─── 监控钱包列表 ─────────────────────────────────────────────────────────────
const WATCH_WALLETS = [
  { name: '阿峰_Afeng', address: '0xbf004bff64725914ee36d03b87d6965b0ced4903' },
  { name: '金狗挖掘机', address: '0x7a2363a401b2340c7941dd2eeff0196a5078d2e6' },
  { name: '枯坐p小将', address: '0x2ce9d43d1cba6ae31d7f07bfe0098dfa2d833373' },
  { name: 'AntPositions(蚂蚁仓）', address: '0xa83b73f5644cde337b61da79589f10ea15548811' },
  { name: 'clukz', address: '0x077b9981bc8a2ca417cea41861111da63266988b' },
  { name: '0xLuck', address: '0x8ade93ba431a2ce19fc62a9ce97626e69a4a333f' },
  { name: 'Jerry', address: '0x76a280376c5332abbbae1786a73c70116906e757' },
  { name: 'Wick李🔶 BN', address: '0x7e8fb0392542812476d9f2d0d71c01d1fa0776c5' },
  { name: 'Yesp 🔶', address: '0xe18c8685818cb936dc2448be33349da61b412a4d' },
  { name: 'Crypto Saba', address: '0xa7d4ffc4eca3c71af150ce302560a9d04a1d2b9f' },
  { name: 'tech', address: '0x3d06315c94ac30b6061c91caf748fc2db04a89f4' },
  { name: 'Zephyr', address: '0xd0f7288a9a4ce03f4a2fdcf5f5806ec21abf0439' },
  { name: '西瓜🔶BNB', address: '0x5388668c4fa8da7f756ef8498ff24d7999999999' },
  { name: '十九岁绿帽少年🍀', address: '0xe7e3773235a7ee4e3b3c1f51a4b516abe888bee6' },
  { name: '不知名打狗大师', address: '0xa1426c6f65fe804e05ce3c07a42412d735ef78bc' },
  { name: '请停止出售 我需要养活我的家人', address: '0x29935ef1417e1c3b1fec68dd666ea9baac31d4ed' },
  { name: 'Han', address: '0x7852346c77b3a622fa73607ee35cc784e53f326b' },
  { name: 'Felix', address: '0x9ba457f105bffac35d67138fecb32a86428c02a2' },
  { name: 'o大（扫链学习版）', address: '0xddac928a240bdace3994c2cc0783d4e29a002127' },
  { name: 'Cowboy🔶BNB', address: '0x38e47fece3ea323e864c65410f6458c820eaa897' },
  { name: '猫叔.𝓒𝓪𝓶𝓮𝓵𝓵𝓲𝓪', address: '0x8d8c031fd095b15f6e7ce1b27c1318015723ae25' },
  { name: 'Cendol 岑铎', address: '0xe9c4c7a243191961af4fd4df44cafe0fafcb2917' },
  { name: '旭旭宝宝', address: '0xcfe30db196c9a99fc80c72ad44fb4a135a53491f' },
  { name: 'lee', address: '0x4866015cc5f98911757e4d63fdfd859f594aef42' },
  { name: '亏的完但赚不完86', address: '0x48363d5e80bb99064b0a67bcb2b2e2e4ea4a48d0' },
  { name: '凯KAI', address: '0x7c57d5632abe5027950f49c4619563dd068db1e7' },
  { name: 'Lord', address: '0x880fcc236bf42d5035bf583dfe2b07dbceb53759' },
  { name: 'CryptoLucky🔶', address: '0x92c371744e71dbe58c603661ba8784fd76472e1b' },
  { name: '被蜗牛追杀中', address: '0xb1e65ccc2f5d68e447bf321ba9bfba35a94f5ad5' },
  { name: 'Ed_x區塊日記', address: '0xc2c6acd377458010713e733e1b21dd6f670d091c' },
  { name: '内盘聪明钱1', address: '0x620435de86211e33b00bff50acfcc0f2022b156b' },
  { name: 'yukaz', address: '0xa05ec35f7d1eba823cff2ed26aeaed419683742f' },
  { name: '0x高达', address: '0xcb099f7d28dc611bde3b0f58e9194b621eeee8ed' },
  { name: '从零开始的打狗生活', address: '0xb90d9ea599c2634069ae4d5eecc5ab7234a81a05' },
  { name: 'CC 小猪🐷', address: '0xf7a934528d38e2f2865a7cd30c335c6d115928b8' },
  { name: 'TOM 🦞', address: '0x170332b75c0859a39bf7288f6cbf0db94bb1f567' },
  { name: 'YOLO', address: '0x61e1de40854cae288a11feb9b28a064df14d29ef' },
  { name: 'CoCo❕', address: '0x5250dbcf6ac11c4c2cbd8dcaf5a6d019f3268ea8' },
  { name: '游侠🔶Yoxia', address: '0xfea3157ff571174f05fc86af3caee3b870a8495a' },
  { name: '深情丨先信🔶BNB', address: '0xd216cf8ee73da8438a3e57dc63703043cfb6e075' },
  { name: '想吃鸡腿🍗', address: '0xfe631cd3c9f7e879f936515265302677805f87b9' },
  { name: '小财神🔶BNB', address: '0xfb4e4fa492217d8401aa9e893c78707b61923953' },
  { name: '我肯定会发财！', address: '0xee21f177eb494646867f4f263ef4075ab151f133' },
  { name: 'Kagami', address: '0x7592ca1ad468ddac5a97a5625c1c55e36338f786' },
  { name: '0x3', address: '0x93c883963af898ab7c41ef9250f9eed71506eb52' },
  { name: '我', address: '0x40cebc3e0c999914002ecbf3384a2cd54ab7b01d' },
];

module.exports = {
  NETWORK,
  API,
  TRADE,
  WATCH_WALLETS,
};
